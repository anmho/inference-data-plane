use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const GPU_GENERATE_WORKFLOW: &str = "GpuGenerateWorkflow";
pub const GPU_STREAM_GENERATE_WORKFLOW: &str = "GpuStreamGenerateWorkflow";
pub const GPU_TASK_QUEUE: &str = "inference-gpu";

pub const ACTIVITY_ADMIT_REQUEST: &str = "AdmitRequest";
pub const ACTIVITY_ENSURE_GPU_ENDPOINT: &str = "EnsureCloudRunGpuEndpoint";
pub const ACTIVITY_GENERATE: &str = "GenerateWithCloudRunGpu";
pub const ACTIVITY_RECONCILE_QUOTA: &str = "ReconcileQuota";
pub const ACTIVITY_RELEASE_IDLE_GPU: &str = "ReleaseIdleCloudRunGpu";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerateWorkflowInput {
    pub request_id: Uuid,
    pub api_key_hash: String,
    pub model: String,
    pub cache_key: String,
    pub prompt_tokens: u64,
    pub max_tokens: u64,
    pub stream: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerateWorkflowResult {
    pub request_id: Uuid,
    pub endpoint: GpuEndpoint,
    pub generated_tokens: u64,
    pub finish_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuEndpoint {
    pub service_name: String,
    pub url: String,
    pub gpu_type: GpuType,
    pub min_instances: u32,
    pub max_instances: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GpuType {
    NvidiaL4,
    NvidiaRtxPro6000,
}

impl GpuType {
    pub fn cloud_run_name(self) -> &'static str {
        match self {
            Self::NvidiaL4 => "nvidia-l4",
            Self::NvidiaRtxPro6000 => "nvidia-rtx-pro-6000",
        }
    }

    pub fn minimum_cpu(self) -> u32 {
        match self {
            Self::NvidiaL4 => 4,
            Self::NvidiaRtxPro6000 => 20,
        }
    }

    pub fn minimum_memory_gib(self) -> u32 {
        match self {
            Self::NvidiaL4 => 16,
            Self::NvidiaRtxPro6000 => 80,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpuSession {
    pub request_id: Uuid,
    pub state: GpuSessionState,
    pub generated_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GpuSessionState {
    Queued,
    Admitted,
    EndpointReady,
    Generating,
    Reconciled,
    Released,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuSessionEvent {
    Admit,
    EndpointReady,
    GenerationStarted,
    GenerationFinished { generated_tokens: u64 },
    QuotaReconciled,
    IdleReleaseFinished,
    Fail,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GpuSessionError {
    #[error("invalid gpu session transition from {from:?} with {event:?}")]
    InvalidTransition {
        from: GpuSessionState,
        event: GpuSessionEvent,
    },
}

impl GpuSession {
    pub fn new(request_id: Uuid) -> Self {
        Self {
            request_id,
            state: GpuSessionState::Queued,
            generated_tokens: 0,
        }
    }

    pub fn apply(&mut self, event: GpuSessionEvent) -> Result<(), GpuSessionError> {
        let next = match (self.state, event) {
            (GpuSessionState::Queued, GpuSessionEvent::Admit) => GpuSessionState::Admitted,
            (GpuSessionState::Admitted, GpuSessionEvent::EndpointReady) => {
                GpuSessionState::EndpointReady
            }
            (GpuSessionState::EndpointReady, GpuSessionEvent::GenerationStarted) => {
                GpuSessionState::Generating
            }
            (
                GpuSessionState::Generating,
                GpuSessionEvent::GenerationFinished { generated_tokens },
            ) => {
                self.generated_tokens = generated_tokens;
                GpuSessionState::Reconciled
            }
            (GpuSessionState::Reconciled, GpuSessionEvent::QuotaReconciled) => {
                GpuSessionState::Released
            }
            (GpuSessionState::Released, GpuSessionEvent::IdleReleaseFinished) => {
                GpuSessionState::Released
            }
            (_, GpuSessionEvent::Fail) => GpuSessionState::Failed,
            (from, event) => {
                return Err(GpuSessionError::InvalidTransition { from, event });
            }
        };
        self.state = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_run_gpu_minimums_match_supported_types() {
        assert_eq!(GpuType::NvidiaL4.cloud_run_name(), "nvidia-l4");
        assert_eq!(GpuType::NvidiaL4.minimum_cpu(), 4);
        assert_eq!(GpuType::NvidiaL4.minimum_memory_gib(), 16);
        assert_eq!(
            GpuType::NvidiaRtxPro6000.cloud_run_name(),
            "nvidia-rtx-pro-6000"
        );
        assert_eq!(GpuType::NvidiaRtxPro6000.minimum_cpu(), 20);
        assert_eq!(GpuType::NvidiaRtxPro6000.minimum_memory_gib(), 80);
    }

    #[test]
    fn gpu_session_reaches_released_state_after_generation() {
        let mut session = GpuSession::new(Uuid::nil());
        session.apply(GpuSessionEvent::Admit).unwrap();
        session.apply(GpuSessionEvent::EndpointReady).unwrap();
        session.apply(GpuSessionEvent::GenerationStarted).unwrap();
        session
            .apply(GpuSessionEvent::GenerationFinished {
                generated_tokens: 42,
            })
            .unwrap();
        session.apply(GpuSessionEvent::QuotaReconciled).unwrap();
        session.apply(GpuSessionEvent::IdleReleaseFinished).unwrap();
        assert_eq!(session.state, GpuSessionState::Released);
        assert_eq!(session.generated_tokens, 42);
    }

    #[test]
    fn gpu_session_rejects_skipping_endpoint_readiness() {
        let mut session = GpuSession::new(Uuid::nil());
        session.apply(GpuSessionEvent::Admit).unwrap();
        let err = session
            .apply(GpuSessionEvent::GenerationStarted)
            .unwrap_err();
        assert_eq!(
            err,
            GpuSessionError::InvalidTransition {
                from: GpuSessionState::Admitted,
                event: GpuSessionEvent::GenerationStarted,
            }
        );
    }
}
