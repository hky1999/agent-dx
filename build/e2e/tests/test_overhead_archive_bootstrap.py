"""Strict, offline full-ledger bootstrap contract; no live Redis mutation."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "overhead_archive", Path(__file__).parents[2] / "maintenance/overhead_archive.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def hold(retired=None, owner="a"):
    return {"host_id": "host", "owner_sha256": owner * 64,
            "resources": {"cpu_millis": 10, "memory_bytes": 16, "disk_bytes": 32},
            "retirement_proof_sha256": retired}


def ledger():
    header = {"schema": 12, "shards": 1, "epoch": 63, "generation": 415,
              "revision": 100, "next_node_shard": 0,
              "host_budgets": {"host": {"capacity": {"cpu_millis": 100,
                  "memory_bytes": 128, "disk_bytes": 256}, "nodes": ["node"]}},
              "overhead_budget": True, "parking_journals": True,
              "queued_recovery": True}
    return {"header": m.canonical(header),
            "node:node": m.canonical({"scheduling_paused": True}),
            "overhead:control": m.canonical(hold()),
            "overhead:old": m.canonical(hold("b" * 64))}


class ArchiveBootstrap(unittest.TestCase):
    def plan(self, raw=None, **kw):
        raw = ledger() if raw is None else raw
        archive = m.make_archive(raw, "adx:{test}:control:v1", 63)
        args = dict(raw=raw, authority="adx:{test}:control:v1", epoch=63,
                    archive=archive, acceptance_sha256="c" * 64,
                    build_id=f"oh-{archive['generation']}-bootstrap",
                    build_reservation=hold())
        args.update(kw)
        return m.bootstrap_plan(**args)

    def test_archives_only_retired_keeps_active_bytes_and_funds_build(self):
        raw = ledger()
        p = self.plan(raw)
        self.assertEqual(p["deleted_fields"], ["overhead:old"])
        self.assertEqual(p["set_fields"]["overhead:control"] if
                         "overhead:control" in p["set_fields"] else raw["overhead:control"],
                         raw["overhead:control"])
        self.assertEqual(len(p["set_fields"]), 1)
        h = m.json.loads(p["next_header"])
        self.assertEqual(h["schema"], 13)
        self.assertEqual(h["epoch"], 63)
        self.assertEqual(h["generation"], 415)
        self.assertEqual(h["revision"], 101)
        self.assertEqual(h["overhead_history"]["retired_count"], 1)
        self.assertEqual(h["overhead_history"]["generation"], 1)
        self.assertEqual(h["overhead_history"]["acceptance_sha256"], "c" * 64)
        self.assertEqual(p["expected_header"], raw["header"])
        self.assertEqual(p["held_after"], {
            "host": {"cpu_millis": 20, "memory_bytes": 32, "disk_bytes": 64}})

    def test_512_records_become_seven_active_and_one_build(self):
        raw = ledger()
        del raw["overhead:old"]
        for i in range(505):
            raw[f"overhead:retired-{i}"] = m.canonical(hold("b" * 64))
        for i in range(6):
            raw[f"overhead:active-{i}"] = m.canonical(hold())
        p = self.plan(raw)
        self.assertEqual(len(p["deleted_fields"]), 505)
        self.assertEqual(len(raw) - 2 - len(p["deleted_fields"]) + len(p["set_fields"]), 8)
        self.assertEqual(p["held_after"]["host"]["cpu_millis"], 80)

    def test_legacy_future_generation_cannot_be_reused(self):
        raw = ledger()
        raw["overhead:oh-900-future"] = raw.pop("overhead:old")
        p = self.plan(raw)
        self.assertEqual(m.json.loads(p["next_header"])["overhead_history"]["generation"], 901)
        self.assertIn("overhead:oh-901-bootstrap", p["set_fields"])

    def test_archive_is_immutable_canonical_and_bound_to_authority(self):
        raw = ledger()
        a = m.make_archive(raw, "adx:{test}:control:v1", 63)
        self.assertEqual(m.archive_digest(a), m.hashlib.sha256(
            m.canonical(a).encode()).hexdigest())
        a["authority"] = "other"
        with self.assertRaises(ValueError):
            self.plan(raw, archive=a)

    def test_archive_cannot_omit_or_forge_retirement(self):
        raw = ledger()
        a = m.make_archive(raw, "adx:{test}:control:v1", 63)
        a["reservations"]["old"]["retirement_proof_sha256"] = "d" * 64
        with self.assertRaises(ValueError):
            self.plan(raw, archive=a)

    def test_live_record_cannot_be_archived(self):
        raw = ledger()
        a = m.make_archive(raw, "adx:{test}:control:v1", 63)
        a["reservations"]["control"] = hold()
        with self.assertRaises(ValueError):
            self.plan(raw, archive=a)

    def test_old_epoch_and_noncanonical_generation_rejected(self):
        with self.assertRaises(ValueError):
            self.plan(epoch=62)
        for name in ["bootstrap", "oh-0-bootstrap", "oh-01-bootstrap",
                     "oh-2-bootstrap", "oh-1-", "oh-1-space x"]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.plan(build_id=name)

    def test_unknown_owner_and_underfunding_rejected(self):
        h = hold()
        h["host_id"] = "unknown"
        with self.assertRaises(ValueError):
            self.plan(build_reservation=h)
        for resource in ["cpu_millis", "memory_bytes", "disk_bytes"]:
            h = hold()
            h["resources"][resource] = 257
            with self.subTest(resource=resource), self.assertRaises(ValueError):
                self.plan(build_reservation=h)

    def test_acceptance_required_and_build_must_be_active(self):
        for digest in ["", "a" * 63, "A" * 64]:
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                self.plan(acceptance_sha256=digest)
        with self.assertRaises(ValueError):
            self.plan(build_reservation=hold("b" * 64))
        with self.assertRaises(ValueError):
            self.plan(build_id="control")

    def test_environment_unknown_fields_and_unpaused_node_rejected(self):
        for field, value in [("environment:x", "{}"), ("unexpected", "{}"),
                             ("node:node", m.canonical({"scheduling_paused": False}))]:
            raw = ledger()
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.plan(raw)

    def test_no_retired_record_and_overfull_input_rejected(self):
        raw = ledger()
        del raw["overhead:old"]
        with self.assertRaises(ValueError):
            self.plan(raw)
        for i in range(512):
            raw[f"overhead:old-{i}"] = m.canonical(hold("b" * 64))
        with self.assertRaises(ValueError):
            self.plan(raw)

    def test_repeated_bootstrap_cannot_roll_back_schema(self):
        raw = ledger()
        p = self.plan(raw)
        raw["header"] = p["next_header"]
        with self.assertRaises(ValueError):
            self.plan(raw)

    def test_invalid_resource_boolean_overflow_and_owner_rejected(self):
        for value in [True, -1, 2**64, 1.5]:
            h = hold()
            h["resources"]["memory_bytes"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.plan(build_reservation=h)
        h = hold()
        h["owner_sha256"] = "no-proof"
        with self.assertRaises(ValueError):
            self.plan(build_reservation=h)

    def test_revision_and_generation_overflow_rejected(self):
        raw = ledger()
        h = m.json.loads(raw["header"])
        h["revision"] = 2**64 - 1
        raw["header"] = m.canonical(h)
        with self.assertRaises(ValueError):
            self.plan(raw)
        raw = ledger()
        raw["overhead:oh-18446744073709551615-old"] = raw.pop("overhead:old")
        with self.assertRaises(ValueError):
            self.plan(raw)

    def test_changed_raw_snapshot_cannot_commit_old_archive(self):
        raw = ledger()
        a = m.make_archive(raw, "adx:{test}:control:v1", 63)
        raw["overhead:old"] = m.canonical(hold("d" * 64))
        with self.assertRaises(ValueError):
            self.plan(raw, archive=a)

    def test_retired_identity_is_not_silently_ignored_when_malformed(self):
        for mutate in ["unknown", "missing", "host"]:
            raw = ledger()
            r = hold("b" * 64)
            if mutate == "unknown":
                r["extra"] = True
            elif mutate == "missing":
                del r["owner_sha256"]
            else:
                r["host_id"] = "unknown"
            raw["overhead:old"] = m.canonical(r)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.plan(raw)


    def test_protected_retired_custody_reference_is_preserved(self):
        raw = ledger()
        raw["overhead:other"] = m.canonical(hold("d" * 64))
        archive = m.make_archive(raw, "adx:{test}:control:v1", 63, ["old"])
        self.assertEqual(archive["protected_ids"], ["old"])
        self.assertEqual(set(archive["reservations"]), {"other"})
        plan = self.plan(raw, archive=archive)
        self.assertEqual(plan["deleted_fields"], ["overhead:other"])
        self.assertNotIn("overhead:old", plan["set_fields"])

    def test_unknown_or_duplicate_protected_id_rejected(self):
        raw = ledger()
        for ids in [["missing"], ["old", "old"], ["space x"]]:
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                m.make_archive(raw, "adx:{test}:control:v1", 63, ids)

    def test_bool_and_float_header_integers_rejected(self):
        for key, value in [("schema", 12.0), ("epoch", 63.0),
                           ("generation", True), ("revision", True)]:
            raw = ledger()
            header = m.json.loads(raw["header"])
            header[key] = value
            raw["header"] = m.canonical(header)
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.plan(raw)

    def test_archive_boolean_cannot_alias_integer_format(self):
        raw = ledger()
        archive = m.make_archive(raw, "adx:{test}:control:v1", 63)
        archive["format"] = True
        with self.assertRaises(ValueError):
            self.plan(raw, archive=archive)

    def test_unknown_header_and_unobserved_host_node_rejected(self):
        for mode in ["unknown", "missing-node"]:
            raw = ledger()
            header = m.json.loads(raw["header"])
            if mode == "unknown":
                header["unknown"] = True
            else:
                header["host_budgets"]["host"]["nodes"].append("unobserved")
            raw["header"] = m.canonical(header)
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.plan(raw)



    def test_protection_is_durable_in_the_published_chain_head(self):
        raw = ledger()
        raw["overhead:other"] = m.canonical(hold("d" * 64))
        archive = m.make_archive(raw, "adx:{test}:control:v1", 63, ["old"])
        plan = self.plan(raw, archive=archive)
        history = m.json.loads(plan["next_header"])["overhead_history"]
        self.assertEqual(history.get("protected_ids", []), ["old"])


if __name__ == "__main__":
    unittest.main()
