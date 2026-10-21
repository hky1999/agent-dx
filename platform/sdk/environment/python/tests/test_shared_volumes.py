"""Metadata SDK tests; mock HTTP does not prove real mounts or filesystem durability."""

import json
import unittest
from unittest.mock import patch

import httpx

from adx_environment import ConnectionConfig, EnvironmentError, SharedVolumes

ID = "00000000-0000-0000-0000-000000000001"
REQUEST = "00000000-0000-0000-0000-000000000002"


def record(**changes):
    value = {
        "id": ID,
        "ownerTenantId": "owner",
        "permissionRevision": "1",
        "grants": [],
    }
    value.update(changes)
    return value


class VolumeSDKTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.result = record()
        self.status = 200
        self.failure = False

        def respond(request):
            self.requests.append(request)
            if self.failure:
                raise httpx.ReadTimeout("lost reply", request=request)
            return httpx.Response(self.status, json=self.result)

        self.http = httpx.Client(transport=httpx.MockTransport(respond))
        self.patch = patch(
            "adx_environment._transport.acquire_shared_http_client",
            return_value=self.http,
        )
        self.acquire = self.patch.start()
        self.client = SharedVolumes(
            connection=ConnectionConfig(
                server_address="localhost:9443",
                token="test-volume-token-not-a-real-credential",
                use_tls=True,
                verify_tls=True,
            )
        )
        self.addCleanup(self.patch.stop)
        self.addCleanup(self.client.close)

    def test_uses_existing_verified_tls_and_token_pool(self):
        args = self.acquire.call_args.args
        self.assertEqual(args[:3], ("https", "localhost:9443", True))
        self.assertEqual(args[3](), "test-volume-token-not-a-real-credential")

    def test_creation_reuses_explicit_identity_without_owner_override(self):
        self.assertEqual(self.client.create(request_id=REQUEST), record())
        self.client.create(request_id=REQUEST)
        bodies = [json.loads(request.content) for request in self.requests]
        self.assertEqual(bodies, [{"requestId": REQUEST}, {"requestId": REQUEST}])
        self.assertEqual(self.requests[0].headers["X-Request-ID"], REQUEST)

    def test_get_validates_returned_identity(self):
        self.assertEqual(self.client.get(ID)["id"], ID)
        self.result = record(id=REQUEST)
        with self.assertRaises(EnvironmentError):
            self.client.get(ID)

    def test_full_width_revisions_are_decimal_strings(self):
        revision = (1 << 64) - 1
        self.result = record(permissionRevision=str(revision))
        self.client.grant(ID, "reader", expected_permission_revision=revision)
        self.assertEqual(
            json.loads(self.requests[0].content)["expectedPermissionRevision"],
            str(revision),
        )
        self.assertEqual(json.loads(self.requests[0].content)["access"], "read_only")

    def test_unknown_creation_outcome_is_not_blindly_replayed(self):
        self.failure = True
        with self.assertRaises(EnvironmentError) as error:
            self.client.create(request_id=REQUEST)
        self.assertEqual(error.exception.request_id, REQUEST)
        self.assertEqual(error.exception.outcome, "unknown")
        self.assertEqual(len(self.requests), 1)

    def test_stale_revision_error_is_not_retried(self):
        self.status = 409
        self.result = {"message": "identity or version conflict"}
        with self.assertRaises(EnvironmentError) as error:
            self.client.grant(ID, "reader", expected_permission_revision=1)
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(len(self.requests), 1)

    def test_invalid_identifiers_fail_before_network(self):
        for value in ("../host", "", ID.upper().replace("0", "A", 1), "0" * 32):
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                self.client.get(value)
        self.assertEqual(self.requests, [])

    def test_paths_access_and_revisions_are_validated_before_network(self):
        for subpath in ("/host", "../x", "a//b", "a/.", "a\\b", "a\nb"):
            with self.subTest(subpath=subpath), self.assertRaises(ValueError):
                self.client.grant(
                    ID, "reader", expected_permission_revision=1, subpath=subpath
                )
        for revision in (True, 0, -1, 1.5, 1 << 64):
            with (
                self.subTest(revision=revision),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.client.revoke(ID, "reader", expected_permission_revision=revision)
        with self.assertRaises(ValueError):
            self.client.grant(
                ID, "reader", expected_permission_revision=1, access="unknown"
            )
        self.assertEqual(self.requests, [])

    def test_corrupt_permissions_are_not_silently_accepted(self):
        values = [
            record(permissionRevision=1),
            record(permissionRevision="01"),
            record(grants=None),
            record(hostPath="/etc"),
            record(ownerTenantId=""),
        ]
        grant = {"tenantId": "reader", "subpath": "", "access": "read_only"}
        values.append(record(grants=[grant, grant]))
        for value in values:
            self.result = value
            with self.subTest(value=value), self.assertRaises(EnvironmentError):
                self.client.get(ID)

    def test_close_is_idempotent_and_never_deletes_the_volume(self):
        self.client.close()
        self.client.close()
        with self.assertRaises(EnvironmentError):
            self.client.get(ID)
        self.assertEqual(self.requests, [])


if __name__ == "__main__":
    unittest.main()
