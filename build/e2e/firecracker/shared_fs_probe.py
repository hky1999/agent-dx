#!/usr/bin/env python3
"""Minimal A0 shared_fs orchestration probe against a real ADX deployment.

Drives the real public SDK chain (create/pause/resume/snapshot/clone/delete)
with ``pause_policy.storage=SHARED`` so checkpoints flow through the adxlet
shared_fs adapter onto the JuiceFS-backed volume. No mock backend is used.
This is the A0 component probe, not a distributed-storage acceptance.

Cross-node semantics: the source environment is placed on ``--source-node``
and the clone on ``--target-node`` (defaults ``a0-source`` / ``a0-target``).
Placement is verified through authoritative ``resume()`` results, which
report the node that actually serves the environment — not merely the
requested constraint.

Payload lives under ``/var/task`` so it exercises the private writable layer
(a gVisor ``/tmp`` is a separate tmpfs and would not prove RWFS restore).

The real-store >2GiB multi-round clone check lives in the dedicated Rust
example ``platform/adxlet/examples/shared_fs_volume_probe.rs`` (drives the
production SharedFsCheckpointStore publish+duplicate on the real volume);
this probe deliberately carries no flag for it so a requested-but-skipped
path can never count as passed.

Usage (TLS, default):
  python3 shared_fs_probe.py --endpoint https://... --token-file F \
      --ca C --output DIR [--runtime firecracker|runsc] [--image IMG] \
      [--source-node a0-source --target-node a0-target]

Usage (isolated HTTP private address):
  python3 shared_fs_probe.py --endpoint http://10.x.x.x:PORT --token-file F \
      --isolated-http --output DIR ...
"""

import argparse
import importlib.metadata
import json
import os
import time
import traceback
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--endpoint', required=True)
p.add_argument('--token-file', type=Path, required=True)
p.add_argument('--ca', type=Path)
p.add_argument('--isolated-http', action='store_true',
               help='private-address mode: plain HTTP, use_tls=False (no CA verification)')
p.add_argument('--runtime', default='firecracker', choices=['firecracker', 'runsc'])
p.add_argument('--image',
               help='OCI image; omit to use the deployment runtime_profile fixed rootfs')
p.add_argument('--source-node', default='a0-source')
p.add_argument('--target-node', default='a0-target')
p.add_argument('--shared-root', type=Path,
               default=Path(os.environ.get('ADX_SHARED_ROOT', '/mnt/juicefs/checkpoints')))
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()

if a.isolated_http and a.ca:
    p.error('--ca is not used with --isolated-http')
if not a.isolated_http and not a.ca:
    p.error('--ca is required for the default TLS mode (or pass --isolated-http)')
if a.source_node == a.target_node:
    p.error('--source-node and --target-node must differ (cross-node semantics)')

if not a.isolated_http:
    os.environ['SSL_CERT_FILE'] = str(a.ca.resolve())

from adx_environment import (  # noqa: E402
    CheckpointStorage,
    ConnectionConfig,
    Environment,
    PausePolicy,
    RetentionPolicy,
)

out = a.output.resolve()
out.mkdir(parents=True, exist_ok=False)
result = {
    'status': 'failed',
    'sdk_version': importlib.metadata.version('adx-environment'),
    'cases': [],
    'runtime': a.runtime,
    'shared_root': str(a.shared_root),
    'cleanup_errors': [],
    'retained': [],
}


def passed(name, **details):
    print('PASS', name, json.dumps(details), flush=True)
    result['cases'].append({'name': name, 'passed': True, **details})


def shared_directories():
    """Final immutable checkpoint directories (bare <uuid>) under the shared root."""
    import uuid as _uuid
    names = []
    try:
        entries = list(a.shared_root.iterdir())
    except FileNotFoundError:
        return names
    for entry in entries:
        name = entry.name
        if name.startswith('.'):
            continue
        try:
            _uuid.UUID(name)
        except ValueError:
            continue
        if entry.is_dir():
            names.append(name)
    return names


def wait_counter(read, minimum, timeout=60.0):
    """Bounded polling until the counter strictly exceeds ``minimum``."""
    deadline = time.monotonic() + timeout
    last = minimum
    while time.monotonic() < deadline:
        try:
            last = int(read())
        except Exception:
            last = minimum
        if last > minimum:
            return last
        time.sleep(0.5)
    raise AssertionError(f'counter did not progress beyond {minimum} within {timeout}s (last={last})')


def timed(name):
    """A0 minimal timing (SDK/RPC wall clock, perf_counter).

    This is NOT a precise guest-pause measurement: each window covers the
    client-side SDK call including RPC, adxlet processing, runtime work and
    (for shared publication) the durability barrier. Precise pause-phase
    attribution is an A3 deliverable; no optimization or cross-product
    comparison is drawn from these numbers.
    """
    class _Timer:
        def __enter__(self):
            self.start = time.perf_counter()
            return self

        def __exit__(self, *exc):
            self.elapsed_ms = round((time.perf_counter() - self.start) * 1000, 1)
            result.setdefault('timings_ms', {})[name] = self.elapsed_ms
            print(f'TIMING {name} elapsed_ms={self.elapsed_ms}', flush=True)
    return _Timer()


def cleanup_step(description, action):
    """Run one cleanup action; record failures instead of swallowing them."""
    try:
        action()
    except Exception as error:  # noqa: BLE001 - cleanup must never mask the original failure
        traceback.print_exc()
        result['cleanup_errors'].append({'step': description, 'error': repr(error)})
        result['retained'].append(description)


if a.isolated_http:
    connection = ConnectionConfig(
        server_address=a.endpoint,
        token=a.token_file.read_text().strip(),
        use_tls=False,
    )
else:
    connection = ConnectionConfig(
        server_address=a.endpoint,
        token=a.token_file.read_text().strip(),
        use_tls=True,
        verify_tls=True,
    )

environment = None
snapshot = None
clone = None

try:
    before_shared = set(shared_directories())

    # SHARED storage policy is fixed at creation time (the SDK pause() call
    # takes no per-call override).
    create_kwargs = dict(
        runtime=a.runtime,
        cpu=500,
        memory=512,
        idle_timeout=0,
        node_id=a.source_node,
        pause_policy=PausePolicy(
            retention=RetentionPolicy.for_seconds(600),
            timeout_seconds=120,
            storage=CheckpointStorage.SHARED,
        ),
        connection=connection,
        create_timeout=180,
    )
    if a.image:
        create_kwargs['image'] = a.image
    environment = Environment(**create_kwargs)
    result['environment_id'] = environment.id
    passed('source environment created on requested node',
           environment_id=environment.id, requested_node=a.source_node)

    def command(script):
        value = environment.commands.run(script)
        if value.exit_code != 0:
            raise AssertionError(value)
        return value.stdout.strip()

    def clone_command(script):
        value = clone.commands.run(script)
        if value.exit_code != 0:
            raise AssertionError(value)
        return value.stdout.strip()

    command('mkdir -p /var/task')
    command(
        "sh -c 'echo $$ >/tmp/counter.pid; n=0; while :; do n=$((n+1)); "
        "echo $n >/tmp/counter; sleep 0.1; done' >/tmp/counter.log 2>&1 </dev/null &"
    )
    time.sleep(1)
    before = int(command('cat /tmp/counter'))
    pid = command('cat /tmp/counter.pid')
    payload = b'a0-shared-fs-payload\x00\xff' * 4096
    environment.files.write('/var/task/a0.bin', payload)
    passed('source workload started', before=before, pid=pid)

    with timed('pause_shared'):
        paused = environment.pause(
            retention=RetentionPolicy.for_seconds(600),
            timeout_seconds=120,
        )
    if not (paused.environment_id == environment.id and paused.size > 0):
        raise AssertionError(paused)
    after_shared = set(shared_directories())
    new_shared = after_shared - before_shared
    if not new_shared:
        raise AssertionError('no new immutable directory under shared root after SHARED pause')
    passed('shared pause published immutable checkpoint directory',
           size=paused.size, new_directories=sorted(new_shared))

    with timed('resume'):
        resumed = environment.resume()
    if resumed.environment_id != environment.id:
        raise AssertionError(resumed)
    if resumed.node_id != a.source_node:
        raise AssertionError(
            f'source actually resumed on {resumed.node_id}, expected {a.source_node}'
        )
    after = wait_counter(lambda: command('cat /tmp/counter'), before)
    if command('cat /tmp/counter.pid') != pid:
        raise AssertionError('resume did not continue the captured process')
    if environment.files.read('/var/task/a0.bin', format='bytes') != payload:
        raise AssertionError('private-layer payload changed across pause/resume')
    result['source_node_verified'] = resumed.node_id
    passed('resume preserved process and private-layer file',
           before=before, after=after, pid=pid, node=resumed.node_id)

    with timed('create_snapshot'):
        snapshot = environment.create_snapshot(name='a0-shared-fs', timeout_seconds=120)
    counter_at_snapshot = wait_counter(lambda: command('cat /tmp/counter'), after)
    passed('reusable snapshot created on shared volume',
           snapshot_id=snapshot.snapshot_id, counter=counter_at_snapshot)

    # The source mutates its private payload after the snapshot: the clone
    # must restore the pre-snapshot version, proving the published parent
    # version is immutable and the branch is independent.
    source_new = b'source-mutated-after-snapshot' * 512
    environment.files.write('/var/task/a0.bin', source_new)
    if environment.files.read('/var/task/a0.bin', format='bytes') != source_new:
        raise AssertionError('source mutation not visible to the source')
    source_later = wait_counter(lambda: command('cat /tmp/counter'), counter_at_snapshot)
    passed('source mutated private payload after snapshot', counter=source_later)

    clone_kwargs = dict(
        idle_timeout=0,
        node_id=a.target_node,
        connection=connection,
        create_timeout=240,
    )
    if a.image:
        clone_kwargs['image'] = a.image
    with timed('snapshot_clone_create'):
        clone = Environment.create(snapshot, **clone_kwargs)
    result['clone_id'] = clone.id
    if clone.id == environment.id:
        raise AssertionError('clone shares source identity')
    with timed('target_first_payload_read'):
        first_payload = clone.files.read('/var/task/a0.bin', format='bytes')
    if first_payload != payload:
        raise AssertionError('clone did not restore the pre-snapshot payload (parent version changed?)')
    clone_command('mkdir -p /var/task')
    clone_pid = clone.commands.run('cat /tmp/counter.pid').stdout.strip()
    if clone_pid != pid:
        raise AssertionError(f'clone process identity changed: {clone_pid} != {pid}')
    # Verify actual placement through an authoritative resume round-trip.
    clone.pause(retention=RetentionPolicy.for_seconds(300), timeout_seconds=120)
    clone_resumed = clone.resume()
    if clone_resumed.node_id != a.target_node:
        raise AssertionError(
            f'clone actually resumed on {clone_resumed.node_id}, expected {a.target_node}'
        )
    result['target_node_verified'] = clone_resumed.node_id
    passed('clone restored pre-snapshot payload on target node',
           clone_id=clone.id, pid=clone_pid, node=clone_resumed.node_id)

    clone.files.write('/var/task/a0.bin', b'clone-only-write')
    if clone.files.read('/var/task/a0.bin', format='bytes') != b'clone-only-write':
        raise AssertionError('clone write not visible in clone')
    if environment.files.read('/var/task/a0.bin', format='bytes') != source_new:
        raise AssertionError('clone write leaked into the live source')
    source_final = wait_counter(lambda: command('cat /tmp/counter'), source_later)
    passed('clone writes independent; source continues with its own mutation',
           source_counter=source_final)

    # Source exits; the live clone keeps its state...
    cleanup_step('kill source environment', lambda: (environment.kill(), environment.close()))
    environment = None
    if clone.files.read('/var/task/a0.bin', format='bytes') != b'clone-only-write':
        raise AssertionError('clone lost its state after source exit')
    passed('source exited; running clone state intact')

    # ...and the ORIGINAL snapshot must still serve a completely fresh restore
    # on the target node: a new environment creation goes through a cold
    # materialize from the shared directory (not a warm cache of the already
    # restored clone, and not the clone's own anonymous recovery point).
    fresh_kwargs = dict(
        idle_timeout=0,
        node_id=a.target_node,
        connection=connection,
        create_timeout=240,
    )
    if a.image:
        fresh_kwargs['image'] = a.image
    with timed('fresh_clone_create'):
        fresh = Environment.create(snapshot, **fresh_kwargs)
    result['fresh_clone_id'] = fresh.id
    try:
        if fresh.id == clone.id or fresh.id == result['environment_id']:
            raise AssertionError('fresh clone reused an existing identity')
        if fresh.files.read('/var/task/a0.bin', format='bytes') != payload:
            raise AssertionError(
                'fresh clone from the original snapshot did not restore the '
                'pre-snapshot payload (parent version mutated or not published?)'
            )
        fresh_resumed_node = None
        fresh.pause(retention=RetentionPolicy.for_seconds(300), timeout_seconds=120)
        fresh_resumed = fresh.resume()
        fresh_resumed_node = fresh_resumed.node_id
        if fresh_resumed_node != a.target_node:
            raise AssertionError(
                f'fresh clone actually resumed on {fresh_resumed_node}, expected {a.target_node}'
            )
        passed('fresh cold clone from original snapshot restored pre-snapshot payload',
               fresh_clone_id=fresh.id, node=fresh_resumed_node)
    finally:
        cleanup_step('kill fresh clone', lambda: (fresh.kill(), fresh.close()))

    Environment.delete_snapshot(snapshot.snapshot_id, connection=connection)
    snapshot = None
    passed('snapshot deleted explicitly (A0: no automatic GC)')

    cleanup_step('kill clone environment', lambda: (clone.kill(), clone.close()))
    clone = None
    if result['cleanup_errors']:
        result['status'] = 'cleanup-failed'
    else:
        result['status'] = 'passed'
except BaseException:
    traceback.print_exc()
finally:
    if clone is not None:
        cleanup_step('final clone teardown', lambda: (clone.kill(), clone.close()))
    if environment is not None:
        cleanup_step('final source teardown', lambda: (environment.kill(), environment.close()))
    if snapshot is not None:
        def _delete_snapshot():
            Environment.delete_snapshot(snapshot.snapshot_id, connection=connection)
        cleanup_step('final snapshot delete', _delete_snapshot)
    (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result['status']), flush=True)

raise SystemExit(0 if result['status'] == 'passed' else 1)
