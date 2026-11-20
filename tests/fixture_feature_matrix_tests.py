#!/usr/bin/env python3
"""Keep Cargo features, CI coverage, and the packaged consumer in sync."""

import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

with (ROOT / "Cargo.toml").open("rb") as cargo_file:
    manifest = tomllib.load(cargo_file)
with (ROOT / ".infra/ci/cargo-matrix.json").open(encoding="utf-8") as matrix_file:
    matrix = json.load(matrix_file)
with (ROOT / "tests/fixtures/all-features-consumer/Cargo.toml").open("rb") as consumer_file:
    consumer = tomllib.load(consumer_file)

root_features = set(manifest["features"]) - {"default"}
checks = {check["name"]: check for check in matrix["checks"]}
assert "all-features" in checks, "cargo matrix must include an all-features row"
all_features = set(checks["all-features"]["features"])
assert all_features == root_features, (
    "all-features matrix row differs from root Cargo features: "
    f"missing={sorted(root_features - all_features)}, "
    f"extra={sorted(all_features - root_features)}"
)
assert checks["all-features"]["defaultFeatures"] is False, "all-features row must disable defaults"
assert "operation" in checks, "cargo matrix must include an operation-only row"
operation = checks["operation"]
assert operation["defaultFeatures"] is False, "operation row must disable defaults"
assert set(operation["features"]) == {"operation"}, "operation row must enable only operation"

consumer_features = consumer.get("features", {})
forwarded = {
    feature
    for feature, dependencies in consumer_features.items()
    if feature != "default" and f"qubit-task/{feature}" in dependencies
}
assert forwarded == root_features, (
    "all-features consumer forwarding differs from root Cargo features: "
    f"missing={sorted(root_features - forwarded)}, "
    f"extra={sorted(forwarded - root_features)}"
)

print("feature matrix and all-features consumer match Cargo.toml")
