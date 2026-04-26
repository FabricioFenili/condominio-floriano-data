from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from floriano.utils.artifacts import (
    ArtifactManifestManager,
    LatestArtifactRegistry,
    sha256_file,
)


def test_sha256_file(tmp_path: Path) -> None:
    path = tmp_path / "a.txt"
    path.write_text("abc", encoding="utf-8")

    assert sha256_file(path) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_latest_artifact_registry_publish_and_read(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.csv"
    artifact.write_text("a,b\n1,2\n", encoding="utf-8")

    registry = LatestArtifactRegistry(base_dir=tmp_path / "_latest")

    pointer_path = registry.publish(
        artifact_name="pdf_text",
        run_id="run-test",
        path=artifact,
        record_count=1,
        status="SUCCESS",
        module="test.module",
    )

    assert pointer_path.exists()

    payload = registry.read("pdf_text")

    assert payload["artifact_name"] == "pdf_text"
    assert payload["run_id"] == "run-test"
    assert payload["record_count"] == 1
    assert payload["status"] == "SUCCESS"


def test_latest_artifact_registry_rejects_non_success(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.csv"
    artifact.write_text("a,b\n1,2\n", encoding="utf-8")

    registry = LatestArtifactRegistry(base_dir=tmp_path / "_latest")

    with pytest.raises(ValueError):
        registry.publish(
            artifact_name="pdf_text",
            run_id="run-test",
            path=artifact,
            record_count=1,
            status="FAILED",
            module="test.module",
        )


def test_artifact_manifest_manager_writes_manifest(tmp_path: Path) -> None:
    manager = ArtifactManifestManager(base_dir=tmp_path / "_manifests")

    path = manager.write_manifest(
        run_id="run-test",
        module="test.module",
        status="SUCCESS",
        input_artifacts=[{"artifact_name": "a"}],
        output_artifacts=[{"artifact_name": "b"}],
        record_counts={"a": 1, "b": 2},
    )

    assert path.exists()

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["run_id"] == "run-test"
    assert payload["module"] == "test.module"
    assert payload["status"] == "SUCCESS"
