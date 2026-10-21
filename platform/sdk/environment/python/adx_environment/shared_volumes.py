"""Durable owner-only volume identity/ACL management, not a mount readiness API."""

from __future__ import annotations

import sys
import uuid
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from ._transport import EnvironmentClient, EnvironmentError
from .types import ConnectionConfig


def _id(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("volume/request id must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError("volume/request id must be a canonical UUID") from error
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("volume/request id must be a canonical nonzero UUID")
    return value


def _tenant(value: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("tenant id must be a nonempty trimmed string")
    if len(value) > 256 or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("invalid tenant id")
    return value


def _subpath(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("subpath must be a string")
    if value and (
        len(value) > 4096
        or "\\" in value
        or any(part in ("", ".", "..") for part in value.split("/"))
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError("subpath must be a canonical relative path or empty root")
    return value


def _revision(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("permission revision must be an integer")
    if not 1 <= value <= (1 << 64) - 1:
        raise ValueError("permission revision must be a positive u64")
    return value


def _access(value: str) -> str:
    if value not in ("read_only", "read_write"):
        raise ValueError("access must be read_only or read_write")
    return value


def _record(value: dict[str, Any], expected: str | None = None) -> dict[str, Any]:
    try:
        if set(value) != {"id", "ownerTenantId", "permissionRevision", "grants"}:
            raise ValueError("invalid volume fields")
        _id(value["id"])
        _tenant(value["ownerTenantId"])
        raw = value["permissionRevision"]
        if not isinstance(raw, str) or not raw.isascii() or not raw.isdecimal():
            raise ValueError("invalid revision encoding")
        revision = _revision(int(raw))
        if str(revision) != raw or (expected is not None and value["id"] != expected):
            raise ValueError("volume identity or revision mismatch")
        grants = value["grants"]
        if not isinstance(grants, list) or len(grants) > 256:
            raise ValueError("invalid grants")
        seen: set[tuple[str, str]] = set()
        for grant in grants:
            if not isinstance(grant, dict) or set(grant) != {
                "tenantId",
                "subpath",
                "access",
            }:
                raise ValueError("invalid grant fields")
            tenant, subpath = _tenant(grant["tenantId"]), _subpath(grant["subpath"])
            _access(grant["access"])
            if tenant == value["ownerTenantId"] or (tenant, subpath) in seen:
                raise ValueError("duplicate or redundant owner grant")
            seen.add((tenant, subpath))
    except (KeyError, TypeError, ValueError) as error:
        raise EnvironmentError("invalid shared volume permission response") from error
    return value


class SharedVolumes:
    """Create/get/conditionally grant or revoke persistent volume metadata.

    The owner is always resolved from the authenticated API key. Grants affect
    new admissions, not already-running mounts. No filesystem deletion or
    public checkpoint catalog is exposed. Close releases only the HTTP lease.
    """

    def __init__(self, *, connection: ConnectionConfig | None = None):
        self._client = EnvironmentClient(connection=connection)
        self._closed = False

    def _request(
        self,
        method: str,
        suffix: str,
        body: dict[str, Any],
        request_id: str,
        expected: str | None = None,
    ) -> dict[str, Any]:
        if self._closed:
            raise EnvironmentError("shared volume client is closed")
        return _record(
            self._client.shared_volume_request(method, suffix, body, request_id),
            expected,
        )

    def create(self, *, request_id: str | None = None) -> dict[str, Any]:
        """Reuse request_id after an unknown outcome; never invent a retry identity."""
        request_id = _id(str(uuid.uuid4()) if request_id is None else request_id)
        return self._request("POST", "", {"requestId": request_id}, request_id)

    def get(self, volume_id: str) -> dict[str, Any]:
        volume_id = _id(volume_id)
        return self._request("GET", f"/{volume_id}", {}, str(uuid.uuid4()), volume_id)

    def grant(
        self,
        volume_id: str,
        tenant_id: str,
        *,
        expected_permission_revision: int,
        access: str = "read_only",
        subpath: str = "",
    ) -> dict[str, Any]:
        volume_id = _id(volume_id)
        return self._request(
            "POST",
            f"/{volume_id}/grant",
            {
                "tenantId": _tenant(tenant_id),
                "subpath": _subpath(subpath),
                "access": _access(access),
                "expectedPermissionRevision": str(
                    _revision(expected_permission_revision)
                ),
            },
            str(uuid.uuid4()),
            volume_id,
        )

    def revoke(
        self,
        volume_id: str,
        tenant_id: str,
        *,
        expected_permission_revision: int,
        subpath: str = "",
    ) -> dict[str, Any]:
        volume_id = _id(volume_id)
        return self._request(
            "POST",
            f"/{volume_id}/revoke",
            {
                "tenantId": _tenant(tenant_id),
                "subpath": _subpath(subpath),
                "expectedPermissionRevision": str(
                    _revision(expected_permission_revision)
                ),
            },
            str(uuid.uuid4()),
            volume_id,
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
