from .probes import HealthcheckProbe, ReadinessProbe
from .shared_volume_mounts import SharedVolumeMount
from .types import (
    CheckpointStorage,
    CommandInfo,
    CommandResult,
    CommandStatus,
    ConnectionConfig,
    DataPlaneSecurityPolicy,
    DNSPolicy,
    DNSRule,
    EntryInfo,
    EnvironmentFailure,
    EnvironmentInfo,
    IdleAction,
    Mount,
    NetworkPolicy,
    NetworkRule,
    NodeInfo,
    PausePolicy,
    PauseResult,
    PortForwarding,
    PortRange,
    RestartPolicy,
    ResumeResult,
    RetentionPolicy,
    S3Config,
    SnapshotInfo,
    TrafficPolicy,
)

SDK_CAPABILITIES = frozenset(
    {
        "shared-volume-management-v1",
        "shared-volume-mount-contract-v1",
        "local-close",
        "explicit-connection-config",
        "tunnel-proxy-port",
        "recoverable-commands",
        "multiplexed-command-watch",
    }
)

__all__ = [
    "SharedVolumeMount",
    "SharedVolumes",
    # current API
    "Environment",
    "ReadinessProbe",
    "HealthcheckProbe",
    "Shell",
    "Shells",
    "CommandHandle",
    "CommandSubmissionError",
    "CommandNotFound",
    "CommandConflict",
    "CommandExpired",
    "CommandWaitTimeout",
    "CommandUnavailable",
    "UnsupportedFeature",
    "ResourceExhausted",
    "EnvironmentNotFound",
    "PermissionDenied",
    "Pty",
    "PtySession",
    "PtyError",
    "EnvironmentError",
    # data types
    "ConnectionConfig",
    "EntryInfo",
    "CommandResult",
    "CommandStatus",
    "CommandInfo",
    "DataPlaneSecurityPolicy",
    "EnvironmentInfo",
    "EnvironmentFailure",
    "SnapshotInfo",
    "PauseResult",
    "ResumeResult",
    "RestartPolicy",
    "PausePolicy",
    "RetentionPolicy",
    "IdleAction",
    "CheckpointStorage",
    "Mount",
    "NetworkPolicy",
    "NetworkRule",
    "PortRange",
    "TrafficPolicy",
    "DNSPolicy",
    "DNSRule",
    "S3Config",
    "PortForwarding",
    "NodeInfo",
    "resources",
    "SDK_CAPABILITIES",
]

# Heavy modules are lazy-loaded so lightweight entry points (the adx-environment CLI)
# don't pay for the httpx/websockets import up front.
_lazy_imports = {
    "SharedVolumes": ".shared_volumes",
    "Environment": ".environment_api",
    "EnvironmentError": "._transport",
    "Shell": ".shell",
    "Shells": ".shell",
    "CommandHandle": ".commands",
    "CommandSubmissionError": ".commands",
    "CommandNotFound": ".commands",
    "CommandConflict": ".commands",
    "CommandExpired": ".commands",
    "CommandWaitTimeout": ".commands",
    "CommandUnavailable": ".commands",
    "UnsupportedFeature": ".commands",
    "ResourceExhausted": ".commands",
    "EnvironmentNotFound": "._transport",
    "PermissionDenied": "._transport",
    "Pty": ".pty",
    "PtySession": ".pty",
    "PtyError": ".pty",
    "PortForwarding": ".types",
    "NodeInfo": ".types",
    "CommandInfo": ".types",
    "DataPlaneSecurityPolicy": ".types",
    "NetworkPolicy": ".types",
    "NetworkRule": ".types",
    "PortRange": ".types",
    "TrafficPolicy": ".types",
    "DNSPolicy": ".types",
    "DNSRule": ".types",
    "resources": "._resources",
}


def __getattr__(name):
    module_path = _lazy_imports.get(name)
    if module_path is not None:
        import importlib

        module = importlib.import_module(module_path, __package__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
