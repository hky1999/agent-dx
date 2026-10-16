//! A dedicated-Redis fault boundary: disable fsync immediately before one CAS.
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::Shutdown,
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub struct FsyncFaultProxy {
    _directory: tempfile::TempDir,
    pub url: String,
    injected: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    streams: Arc<Mutex<Vec<UnixStream>>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

fn command(args: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend(format!("${}\r\n", arg.len()).as_bytes());
        out.extend(arg);
        out.extend(b"\r\n");
    }
    out
}

fn arguments(input: &mut BufReader<UnixStream>) -> io::Result<Option<Vec<Vec<u8>>>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid test RESP command");
    let mut line = String::new();
    if input.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let count: usize = line
        .strip_prefix('*')
        .ok_or_else(invalid)?
        .trim()
        .parse()
        .map_err(|_| invalid())?;
    if !(1..=64).contains(&count) {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        input.read_line(&mut line)?;
        let size: usize = line
            .strip_prefix('$')
            .ok_or_else(invalid)?
            .trim()
            .parse()
            .map_err(|_| invalid())?;
        if size > 1 << 20 {
            return Err(invalid());
        }
        let mut data = vec![0; size];
        input.read_exact(&mut data)?;
        let mut suffix = [0; 2];
        input.read_exact(&mut suffix)?;
        if suffix != *b"\r\n" {
            return Err(invalid());
        }
        out.push(data);
    }
    Ok(Some(out))
}

fn disable_fsync(socket: &Path) -> io::Result<()> {
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    stream.write_all(&command(&[
        b"CONFIG".to_vec(),
        b"SET".to_vec(),
        b"appendfsync".to_vec(),
        b"no".to_vec(),
    ]))?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response)?;
    if response != "+OK\r\n" {
        return Err(io::Error::other("dedicated Redis did not disable fsync"));
    }
    Ok(())
}

impl FsyncFaultProxy {
    pub fn new(redis_url: &str, operation_id: &str) -> Self {
        let socket = Path::new(
            redis_url
                .strip_prefix("redis+unix://")
                .expect("dedicated Redis Unix URL"),
        )
        .to_path_buf();
        let directory = tempfile::Builder::new()
            .prefix("adx-fsync-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = directory.path().join("proxy.sock");
        let listener = UnixListener::bind(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let injected = Arc::new(AtomicBool::new(false));
        let streams: Arc<Mutex<Vec<UnixStream>>> = Arc::default();
        let workers: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let thread_stop = stop.clone();
        let thread_injected = injected.clone();
        let thread_streams = streams.clone();
        let thread_workers = workers.clone();
        let needle = operation_id.as_bytes().to_vec();
        let acceptor = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let client = match listener.accept() {
                    Ok((client, _)) => client,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };
                let Ok(remote) = UnixStream::connect(&socket) else {
                    continue;
                };
                thread_streams
                    .lock()
                    .unwrap()
                    .extend([client.try_clone().unwrap(), remote.try_clone().unwrap()]);
                let mut replies = remote.try_clone().unwrap();
                let mut recipient = client.try_clone().unwrap();
                thread_workers.lock().unwrap().push(thread::spawn(move || {
                    let _ = io::copy(&mut replies, &mut recipient);
                }));
                let needle = needle.clone();
                let socket = socket.clone();
                let injected = thread_injected.clone();
                thread_workers.lock().unwrap().push(thread::spawn(move || {
                    let mut input = BufReader::new(client);
                    let mut output = remote;
                    while let Ok(Some(args)) = arguments(&mut input) {
                        let target = args[0].eq_ignore_ascii_case(b"EVAL")
                            && args
                                .iter()
                                .any(|arg| arg.windows(needle.len()).any(|part| part == needle));
                        if target
                            && injected
                                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                                .is_ok()
                            && disable_fsync(&socket).is_err()
                        {
                            break;
                        }
                        if output.write_all(&command(&args)).is_err() {
                            break;
                        }
                    }
                    let _ = output.shutdown(Shutdown::Both);
                    let _ = input.get_ref().shutdown(Shutdown::Both);
                }));
            }
        });
        Self {
            _directory: directory,
            url: format!("redis+unix://{}", path.display()),
            injected,
            stop,
            acceptor: Some(acceptor),
            streams,
            workers,
        }
    }

    pub fn injected(&self) -> bool {
        self.injected.load(Ordering::Acquire)
    }
}

impl Drop for FsyncFaultProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(acceptor) = self.acceptor.take() {
            acceptor.join().unwrap();
        }
        for stream in self.streams.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
        for worker in self.workers.lock().unwrap().drain(..) {
            worker.join().unwrap();
        }
    }
}
