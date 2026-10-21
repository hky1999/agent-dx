"""Typed shared-volume requests. No host path or arbitrary mount options."""

from __future__ import annotations

import posixpath
from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

MAX_SHARED_VOLUMES = 32


def _path(value: str, *, absolute: bool) -> str:
    if not isinstance(value, str):
        raise TypeError("shared volume path must be a string")
    if not absolute and value == "":
        return value
    if (
        len(value) > 4096
        or "\\" in value
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError("invalid shared volume path")
    if value.startswith("/") != absolute:
        raise ValueError(
            "shared volume target must be absolute; subpath must be relative"
        )
    parts = value[1:].split("/") if absolute else value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("shared volume paths must be canonical")
    if absolute and any(
        value == root or value.startswith(root + "/")
        for root in ("/dev", "/proc", "/sys", "/__adx")
    ):
        raise ValueError("shared volume target is reserved")
    return value


@dataclass(frozen=True)
class SharedVolumeMount:
    """An authorized external volume, not part of the private checkpoint.

    Revocation affects new admissions. Existing writers must be positively
    stopped before a replacement writer can start. Subdirectories must exist.
    """

    volume_id: str
    target: str
    access: str = "read_only"
    subpath: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.volume_id, str):
            raise TypeError("volume_id must be a canonical UUID")
        parsed = UUID(self.volume_id)
        if parsed.int == 0 or str(parsed) != self.volume_id:
            raise ValueError("volume_id must be a canonical nonzero UUID")
        _path(self.target, absolute=True)
        _path(self.subpath, absolute=False)
        if self.access not in ("read_only", "read_write"):
            raise ValueError("access must be read_only or read_write")

    def to_dict(self) -> dict[str, str]:
        return {
            "volumeId": self.volume_id,
            "target": self.target,
            "access": self.access,
            "subpath": self.subpath,
        }


def normalize_shared_mounts(
    values: Sequence[SharedVolumeMount] | None,
    ordinary_targets: Sequence[str] = (),
) -> tuple[SharedVolumeMount, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes)):
        raise TypeError("shared_volumes must contain SharedVolumeMount objects")
    mounts = tuple(values)
    if len(mounts) > MAX_SHARED_VOLUMES:
        raise ValueError("at most 32 shared volumes are supported")
    if any(not isinstance(mount, SharedVolumeMount) for mount in mounts):
        raise TypeError("shared_volumes must contain only SharedVolumeMount objects")
    if len({mount.volume_id for mount in mounts}) != len(mounts):
        raise ValueError("a shared volume cannot occur twice in one execution")
    previous = [posixpath.normpath("/" + path.lstrip("/")) for path in ordinary_targets]
    for mount in mounts:
        for other in previous:
            if (
                mount.target == other
                or mount.target.startswith(other.rstrip("/") + "/")
                or other.startswith(mount.target.rstrip("/") + "/")
            ):
                raise ValueError("shared volume targets must not overlap other mounts")
        previous.append(mount.target)
    return mounts


def require_mount_contract(client: object) -> None:
    """Old servers must fail BEFORE create, never ignore a requested mount."""
    from uuid import uuid4

    from ._transport import EnvironmentError

    probe = getattr(client, "shared_volume_request", None)
    if not callable(probe):
        raise EnvironmentError(
            "installed ADX transport lacks the shared-volume mount contract"
        )
    value = probe("GET", "/mount-contract", {}, str(uuid4()))
    if value != {"version": 1}:
        raise EnvironmentError(
            "server does not support the shared-volume mount contract"
        )
