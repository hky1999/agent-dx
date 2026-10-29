"""Prepare one reviewed schema-12 full-ledger maintenance transition.

This offline tool never connects to Redis. The operator independently persists and
accepts the complete archive before applying the returned opaque-header CAS. Only
one declared build owner may be attached, within the existing physical-host pool.
Ordinary operation after this transition uses the Rust overhead budget interface.
"""
import argparse
import copy
import hashlib
import json
import re
from pathlib import Path

MAX = 2**64 - 1
IDENTITY = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
GENERATION = re.compile(r"oh-([1-9][0-9]*)-([A-Za-z0-9_-]+)\Z")
RESOURCES = {"cpu_millis", "memory_bytes", "disk_bytes"}
RESERVATION = {"host_id", "owner_sha256", "resources", "retirement_proof_sha256"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, positive=False):
    require(type(value) is int and 0 <= value <= MAX and (not positive or value > 0),
            "bounded unsigned integer required")
    return value


def digest(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value),
            "SHA256 required")


def resources(value):
    require(isinstance(value, dict) and set(value) == RESOURCES,
            "complete scalar resources required")
    for key, amount in value.items():
        number(amount, key != "disk_bytes")


def reservation(value, hosts):
    require(isinstance(value, dict) and set(value) == RESERVATION,
            "complete reservation required")
    require(isinstance(value["host_id"], str) and value["host_id"] in hosts,
            "physical host owner required")
    digest(value["owner_sha256"])
    resources(value["resources"])
    if value["retirement_proof_sha256"] is not None:
        digest(value["retirement_proof_sha256"])


def decoded(raw, epoch):
    require(isinstance(raw, dict) and all(isinstance(k, str) and isinstance(v, str)
                                        for k, v in raw.items()), "opaque hash snapshot required")
    header = json.loads(raw["header"])
    number(epoch, True)
    allowed_header = {"schema", "shards", "epoch", "generation", "revision",
                      "next_node_shard", "host_budgets", "overhead_budget",
                      "parking_journals", "queued_recovery", "host_pressure_admission"}
    require(set(header) <= allowed_header, "unknown control header field")
    for key in ["schema", "shards", "epoch", "generation", "revision"]:
        number(header.get(key), key != "generation")
    number(header.get("next_node_shard"))
    require(header["next_node_shard"] < header["shards"], "bounded control shard required")
    require(header.get("epoch") == epoch, "current epoch required")
    require(header.get("schema") == 12 and "overhead_history" not in header,
            "one initial schema-12 transition required")
    number(header.get("revision"), True)
    require(header.get("overhead_budget") is True, "configured overhead budget required")
    hosts = header.get("host_budgets")
    require(isinstance(hosts, dict) and hosts, "physical host budgets required")
    for host, budget in hosts.items():
        require(isinstance(host, str) and host and len(host) <= 128 and host.strip() == host,
                "bounded host identity required")
        resources(budget["capacity"])
    members = set()
    for budget in hosts.values():
        nodes = budget.get("nodes")
        require(isinstance(nodes, list) and nodes and all(
            isinstance(node, str) and node and node.strip() == node and node not in members
            for node in nodes) and len(set(nodes)) == len(nodes), "unique host membership required")
        members.update(nodes)
    require(all("node:" + node in raw for node in members),
            "all configured physical nodes must be observed")
    rows = {}
    for key, value in raw.items():
        if key.startswith("overhead:"):
            identity = key.removeprefix("overhead:")
            require(IDENTITY.fullmatch(identity), "bounded reservation ID required")
            row = json.loads(value)
            reservation(row, hosts)
            rows[identity] = row
        elif key.startswith("node:"):
            require(json.loads(value).get("scheduling_paused") is True,
                    "all participating nodes must be paused")
        elif key.startswith("environment:"):
            raise ValueError("maintenance requires no Environment, including unheld history")
        else:
            require(key == "header" or key.startswith("parking:"),
                    "unknown control field")
    require(0 < len(rows) <= 512, "bounded complete overhead ledger required")
    return header, hosts, rows


def make_archive(raw, authority, epoch, protected_ids=()):
    require(isinstance(authority, str) and authority.startswith("adx:{") and
            authority.endswith("}:control:v1") and len(authority) <= 160,
            "complete Redis authority required")
    _, _, rows = decoded(raw, epoch)
    generations = []
    for identity in rows:
        match = GENERATION.fullmatch(identity)
        if match:
            generations.append(number(int(match[1]), True))
    highest = max(generations, default=0)
    require(highest < MAX, "reservation generation exhausted")
    require(isinstance(protected_ids, (list, tuple)) and len(protected_ids) <= 512
            and len(set(protected_ids)) == len(protected_ids)
            and all(isinstance(v, str) and IDENTITY.fullmatch(v) and v in rows
                    for v in protected_ids), "complete bounded protected identities required")
    retired = {identity: row for identity, row in rows.items()
               if row["retirement_proof_sha256"] is not None and identity not in protected_ids}
    require(retired, "retired ownership receipts required")
    return {"format": 1, "authority": authority, "source_epoch": epoch,
            "generation": highest + 1, "previous": None,
            "protected_ids": sorted(protected_ids), "reservations": retired}


def archive_digest(archive):
    return hashlib.sha256(canonical(archive).encode()).hexdigest()


def bootstrap_plan(*, raw, authority, epoch, archive, acceptance_sha256,
                   build_id, build_reservation):
    expected_archive = make_archive(raw, authority, epoch, archive.get("protected_ids", ()))
    require(canonical(archive) == canonical(expected_archive), "complete matching archive required")
    digest(acceptance_sha256)
    header, hosts, rows = decoded(raw, epoch)
    require(isinstance(build_id, str) and IDENTITY.fullmatch(build_id),
            "bounded build reservation ID required")
    match = GENERATION.fullmatch(build_id)
    require(match is not None and int(match[1]) == archive["generation"],
            "current canonical reservation generation required")
    require(build_id not in rows, "build identity already exists")
    reservation(build_reservation, hosts)
    require(build_reservation["retirement_proof_sha256"] is None,
            "new build must retain its complete live hold")
    held = {host: {key: 0 for key in RESOURCES} for host in hosts}
    for row in [*rows.values(), build_reservation]:
        if row["retirement_proof_sha256"] is not None:
            continue
        current = held[row["host_id"]]
        capacity = hosts[row["host_id"]]["capacity"]
        for key, amount in row["resources"].items():
            current[key] += amount
            require(current[key] <= capacity[key], "host capacity exceeded")
    require(header["revision"] < MAX, "control revision exhausted")
    next_header = copy.deepcopy(header)
    next_header["schema"] = 13
    next_header["revision"] += 1
    next_header["overhead_history"] = {
        "generation": archive["generation"],
        "retired_count": len(archive["reservations"]),
        "protected_ids": archive["protected_ids"],
        "last_archive_sha256": archive_digest(archive),
        "acceptance_sha256": acceptance_sha256}
    return {"authority": authority, "expected_header": raw["header"],
            "next_header": canonical(next_header),
            "deleted_fields": [f"overhead:{identity}"
                               for identity in sorted(archive["reservations"])],
            "set_fields": {f"overhead:{build_id}": canonical(build_reservation)},
            "archive_sha256": archive_digest(archive),
            "acceptance_sha256": acceptance_sha256,
            "held_after": held}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--authority", required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--acceptance-sha256")
    parser.add_argument("--protected-id", action="append", default=[])
    parser.add_argument("--build-id")
    parser.add_argument("--build-reservation", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = json.loads(args.snapshot.read_text())
    if args.acceptance_sha256 is None:
        require(not args.build_id and args.build_reservation is None,
                "prepare does not attach a build owner")
        result = make_archive(raw, args.authority, args.epoch, args.protected_id)
        require(not args.archive.exists(), "archive path must be new")
        with args.archive.open("x") as f:
            f.write(canonical(result))
        result = {"archive_sha256": archive_digest(result),
                  "archive_bytes": args.archive.stat().st_size}
    else:
        require(args.build_id and args.build_reservation, "one build owner required")
        result = bootstrap_plan(raw=raw, authority=args.authority, epoch=args.epoch,
            archive=json.loads(args.archive.read_text()),
            acceptance_sha256=args.acceptance_sha256, build_id=args.build_id,
            build_reservation=json.loads(args.build_reservation.read_text()))
    with args.output.open("x") as f:
        f.write(canonical(result) + "\n")


if __name__ == "__main__":
    main()
