//! Execd Ready notification, or HTTP readiness when no stream is configured.
use crate::runtime_control::RuntimeControlClient;
use crate::{Readiness, RuntimeDriver};
use adx_core::runtime::RuntimePhase;
use adx_core::{EnvironmentRecord, Error, Result};
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct ExecdReadiness {
    runtime: Arc<dyn RuntimeDriver>,
    client: RuntimeControlClient,
    poll_interval: Duration,
    probe_timeout: Duration,
    ready_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ReadinessStage {
    #[default]
    Idle,
    ReadyNotification,
    HttpStatus,
    RuntimeRunning,
}

impl ReadinessStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::ReadyNotification => "ready_notification",
            Self::HttpStatus => "http_status",
            Self::RuntimeRunning => "runtime_running",
        }
    }
}

#[derive(Default)]
struct ReadinessObservations {
    poll_attempts: u64,
    poll_timeouts: u64,
    poll_in_flight: bool,
    ready_notification_seen: bool,
    last_poll_error: Option<String>,
    poll_stage: ReadinessStage,
    last_poll_stage: ReadinessStage,
    notification_stage: ReadinessStage,
}

impl ReadinessObservations {
    fn started(&mut self) {
        self.poll_attempts += 1;
        self.poll_in_flight = true;
        self.poll_stage = ReadinessStage::HttpStatus;
    }

    fn finished(&mut self, result: &std::result::Result<Result<()>, tokio::time::error::Elapsed>) {
        self.poll_in_flight = false;
        self.last_poll_stage = self.poll_stage;
        self.poll_stage = ReadinessStage::Idle;
        match result {
            Ok(Ok(())) => (),
            Ok(Err(error)) => {
                self.last_poll_error = Some(error.to_string().chars().take(1024).collect());
            }
            Err(_) => {
                self.poll_timeouts += 1;
                self.last_poll_error = Some("per-probe deadline exceeded".into());
            }
        }
    }
}

impl ExecdReadiness {
    pub fn new(
        runtime: Arc<dyn RuntimeDriver>,
        port: u16,
        poll_interval: Duration,
        probe_timeout: Duration,
        ready_timeout: Duration,
    ) -> Result<Self> {
        if port == 0
            || poll_interval.is_zero()
            || probe_timeout.is_zero()
            || ready_timeout.is_zero()
        {
            return Err(Error::Invalid(
                "EXECD port and readiness durations must be positive".into(),
            ));
        }
        let client = RuntimeControlClient::new(port, probe_timeout)?;
        Self::with_client(runtime, client, poll_interval, probe_timeout, ready_timeout)
    }

    pub fn with_client(
        runtime: Arc<dyn RuntimeDriver>,
        client: RuntimeControlClient,
        poll_interval: Duration,
        probe_timeout: Duration,
        ready_timeout: Duration,
    ) -> Result<Self> {
        if poll_interval.is_zero() || probe_timeout.is_zero() || ready_timeout.is_zero() {
            return Err(Error::Invalid(
                "EXECD readiness durations must be positive".into(),
            ));
        }
        Ok(Self {
            runtime,
            client,
            poll_interval,
            probe_timeout,
            ready_timeout,
        })
    }

    pub fn with_token(mut self, token: &str) -> Result<Self> {
        self.client = self.client.with_token(token)?;
        Ok(self)
    }
    async fn probe(&self, record: &EnvironmentRecord) -> Result<()> {
        let status = self.client.probe_status(record).await?;
        if let Some(failure) = status.failure {
            return Err(Error::ProbeFailed(failure));
        }
        if status.phase != RuntimePhase::Running {
            return Err(unavailable(format!(
                "runtime control is not running (phase={:?})",
                status.phase
            )));
        }
        Ok(())
    }
}
fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("EXECD readiness: {error}"))
}

#[async_trait]
impl Readiness for ExecdReadiness {
    async fn probe_failure(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<adx_core::probes::ProbeFailure>> {
        Ok(self.client.probe_status(record).await?.failure)
    }
    fn expect_runtime(&self, record: &EnvironmentRecord) -> Result<()> {
        self.client.expect_runtime(record)
    }
    async fn activity(&self, record: &EnvironmentRecord) -> Result<(u64, u64)> {
        let status = self.client.status(record).await?;
        if status.phase != RuntimePhase::Running || status.activity_revision == 0 {
            return Err(unavailable("runtime activity observation is not ready"));
        }
        // A detached background command belongs to the runtime, not to a live
        // client. Its start still advances activity_revision, but it must not
        // indefinitely prevent an otherwise idle capsule from being reaped.
        Ok((status.activity_revision, status.active_requests))
    }
    async fn wait_ready(&self, record: &EnvironmentRecord) -> Result<()> {
        let ready_timeout = if record.spec.execution.init_timeout_seconds > 0 {
            Duration::from_secs(record.spec.execution.init_timeout_seconds)
        } else {
            self.ready_timeout
        };
        let observations = Mutex::new(ReadinessObservations::default());
        let polling = async {
            loop {
                observations
                    .lock()
                    .expect("readiness observation lock poisoned")
                    .started();
                let probe = tokio::time::timeout(self.probe_timeout, async {
                    self.probe(record).await?;
                    observations
                        .lock()
                        .expect("readiness observation lock poisoned")
                        .poll_stage = ReadinessStage::RuntimeRunning;
                    if !self.runtime.is_running(&record.runtime.id).await? {
                        return Err(unavailable("runtime exited during readiness"));
                    }
                    Ok(())
                })
                .await;
                observations
                    .lock()
                    .expect("readiness observation lock poisoned")
                    .finished(&probe);
                if let Ok(Err(error @ Error::ProbeFailed(_))) = &probe {
                    return Err(error.clone());
                }
                if matches!(probe, Ok(Ok(()))) {
                    return Ok(());
                }
                tokio::time::sleep(self.poll_interval).await;
            }
        };
        tokio::time::timeout(ready_timeout, async {
            if self.client.uses_stream() {
                observations
                    .lock()
                    .expect("readiness observation lock poisoned")
                    .notification_stage = ReadinessStage::ReadyNotification;
                tokio::select! {
                    result = self.client.wait_ready_notification(record) => {
                        result?;
                        {
                            let mut observation = observations
                            .lock()
                            .expect("readiness observation lock poisoned");
                            observation.ready_notification_seen = true;
                            observation.notification_stage = ReadinessStage::HttpStatus;
                        }
                        self.probe(record).await?;
                        observations
                            .lock()
                            .expect("readiness observation lock poisoned")
                            .notification_stage = ReadinessStage::RuntimeRunning;
                        if !self.runtime.is_running(&record.runtime.id).await? {
                            return Err(unavailable("runtime exited before Ready"));
                        }
                        observations
                            .lock()
                            .expect("readiness observation lock poisoned")
                            .notification_stage = ReadinessStage::Idle;
                        Ok(())
                    }
                    result = polling => result,
                }
            } else {
                polling.await
            }
        })
        .await
        .map_err(|_| {
            let observation = observations
                .lock()
                .expect("readiness observation lock poisoned");
            let last = observation
                .last_poll_error
                .as_deref()
                .unwrap_or("no completed failing poll");
            adx_observability::warn!(
                event = "environment_readiness_timeout",
                environment_id = %record.spec.id,
                runtime_id = %record.runtime.id,
                generation = record.assignment.generation,
                runtime_ip = ?record.runtime.ip,
                ready_timeout_seconds = ready_timeout.as_secs_f64(),
                poll_attempts = observation.poll_attempts,
                poll_timeouts = observation.poll_timeouts,
                poll_in_flight = observation.poll_in_flight,
                poll_stage = observation.poll_stage.as_str(),
                last_poll_stage = observation.last_poll_stage.as_str(),
                ready_notification_seen = observation.ready_notification_seen,
                ready_notification_stage = observation.notification_stage.as_str(),
                last_poll_error = last,
                "EXECD readiness deadline exceeded"
            );
            unavailable(format!(
                "deadline exceeded; polling attempts={}, timeouts={}, in_flight={}, ready_notification_seen={}; last probe error: {last}",
                observation.poll_attempts,
                observation.poll_timeouts,
                observation.poll_in_flight,
                observation.ready_notification_seen,
            ))
        })?
    }
}

#[cfg(test)]
mod observation_tests {
    use super::{unavailable, ReadinessObservations, ReadinessStage};

    #[test]
    fn pending_poll_distinguishes_http_from_runtime_query() {
        let mut observation = ReadinessObservations::default();
        assert_eq!(observation.poll_stage.as_str(), "idle");
        observation.started();
        assert!(observation.poll_in_flight);
        assert_eq!(observation.poll_stage.as_str(), "http_status");
        observation.poll_stage = ReadinessStage::RuntimeRunning;
        assert_eq!(observation.poll_stage.as_str(), "runtime_running");
        observation.finished(&Ok(Ok(())));
        assert!(!observation.poll_in_flight);
        assert_eq!(observation.poll_stage, ReadinessStage::Idle);
        assert_eq!(observation.last_poll_stage, ReadinessStage::RuntimeRunning);
    }

    #[test]
    fn failed_poll_preserves_last_stage_without_conflating_notification() {
        let mut observation = ReadinessObservations {
            notification_stage: ReadinessStage::ReadyNotification,
            ..ReadinessObservations::default()
        };
        observation.started();
        observation.finished(&Ok(Err(unavailable("status failed"))));
        assert_eq!(observation.last_poll_stage, ReadinessStage::HttpStatus);
        assert_eq!(
            observation.notification_stage.as_str(),
            "ready_notification"
        );
        assert!(!observation.ready_notification_seen);
        assert!(observation.last_poll_error.is_some());
        observation.started();
        observation.poll_stage = ReadinessStage::RuntimeRunning;
        observation.finished(&Ok(Err(unavailable("runtime query failed"))));
        assert_eq!(observation.last_poll_stage, ReadinessStage::RuntimeRunning);
        assert_eq!(observation.poll_attempts, 2);
    }
}
