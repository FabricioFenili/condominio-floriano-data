from __future__ import annotations

import csv
from pathlib import Path

import pytest

from floriano.raw.raw_artifact_consistency import RawArtifactConsistencyChecker
from floriano.utils.execution_logger import PipelineExecutionLogger


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


SOURCE_FIELDS = [
    "run_id",
    "competencia",
    "source_path",
    "sha256",
    "validation_status",
]

TEXT_FIELDS = [
    "run_id",
    "competencia",
    "source_path",
    "source_sha256",
    "page_number",
    "page_count",
    "text_length",
]

LINK_FIELDS = [
    "run_id",
    "competencia",
    "source_path",
    "source_sha256",
    "page_number",
    "uri_sha256",
    "link_type",
    "rect_x0",
    "rect_y0",
    "rect_x1",
    "rect_y1",
]


def create_valid_artifacts(tmp_path: Path) -> tuple[Path, Path, Path]:
    source = tmp_path / "source_inventory.csv"
    text = tmp_path / "pdf_text.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(
        source,
        SOURCE_FIELDS,
        [
            {"run_id": "run-source", "competencia": "2026_01", "source_path": "a.pdf", "sha256": "sha-a", "validation_status": "VALID"},
            {"run_id": "run-source", "competencia": "2026_02", "source_path": "b.pdf", "sha256": "sha-b", "validation_status": "VALID"},
        ],
    )

    write_csv(
        text,
        TEXT_FIELDS,
        [
            {"run_id": "run-text", "competencia": "2026_01", "source_path": "a.pdf", "source_sha256": "sha-a", "page_number": "1", "page_count": "1", "text_length": "10"},
            {"run_id": "run-text", "competencia": "2026_02", "source_path": "b.pdf", "source_sha256": "sha-b", "page_number": "1", "page_count": "1", "text_length": "20"},
        ],
    )

    write_csv(
        links,
        LINK_FIELDS,
        [
            {"run_id": "run-links", "competencia": "2026_01", "source_path": "a.pdf", "source_sha256": "sha-a", "page_number": "1", "uri_sha256": "uri-a", "link_type": "downloadarquivo", "rect_x0": "1", "rect_y0": "1", "rect_x1": "2", "rect_y1": "2"},
            {"run_id": "run-links", "competencia": "2026_02", "source_path": "b.pdf", "source_sha256": "sha-b", "page_number": "1", "uri_sha256": "uri-b", "link_type": "downloadarquivo", "rect_x0": "1", "rect_y0": "1", "rect_x1": "2", "rect_y1": "2"},
        ],
    )

    return source, text, links


def make_checker(tmp_path: Path, source: Path, text: Path, links: Path) -> RawArtifactConsistencyChecker:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-check")

    return RawArtifactConsistencyChecker(
        source_inventory_path=source,
        pdf_text_path=text,
        pdf_links_path=links,
        output_dir=tmp_path / "quality",
        latest_registry_dir=tmp_path / "_latest",
        manifest_dir=tmp_path / "_manifests",
        logger=logger,
        require_parquet=False,
    )


def test_raw_artifact_consistency_success(tmp_path: Path) -> None:
    source, text, links = create_valid_artifacts(tmp_path)
    checker = make_checker(tmp_path, source, text, links)

    outputs = checker.run()

    assert Path(outputs["latest_csv_path"]).exists()
    assert Path(outputs["latest_summary_path"]).exists()
    assert Path(outputs["manifest_path"]).exists()

    summary = Path(outputs["latest_summary_path"]).read_text(encoding="utf-8")
    assert '"approved": true' in summary


def test_raw_artifact_consistency_fails_on_multiple_run_ids(tmp_path: Path) -> None:
    source, text, links = create_valid_artifacts(tmp_path)

    write_csv(
        source,
        SOURCE_FIELDS,
        [
            {"run_id": "run-source-1", "competencia": "2026_01", "source_path": "a.pdf", "sha256": "sha-a", "validation_status": "VALID"},
            {"run_id": "run-source-2", "competencia": "2026_02", "source_path": "b.pdf", "sha256": "sha-b", "validation_status": "VALID"},
        ],
    )

    checker = make_checker(tmp_path, source, text, links)

    with pytest.raises(ValueError):
        checker.run()


def test_raw_artifact_consistency_fails_on_missing_text_competencia(tmp_path: Path) -> None:
    source, text, links = create_valid_artifacts(tmp_path)

    write_csv(
        text,
        TEXT_FIELDS,
        [
            {"run_id": "run-text", "competencia": "2026_01", "source_path": "a.pdf", "source_sha256": "sha-a", "page_number": "1", "page_count": "1", "text_length": "10"},
        ],
    )

    checker = make_checker(tmp_path, source, text, links)

    with pytest.raises(ValueError):
        checker.run()


def test_raw_artifact_consistency_fails_on_duplicate_text_page(tmp_path: Path) -> None:
    source, text, links = create_valid_artifacts(tmp_path)

    write_csv(
        text,
        TEXT_FIELDS,
        [
            {"run_id": "run-text", "competencia": "2026_01", "source_path": "a.pdf", "source_sha256": "sha-a", "page_number": "1", "page_count": "1", "text_length": "10"},
            {"run_id": "run-text", "competencia": "2026_01", "source_path": "a.pdf", "source_sha256": "sha-a", "page_number": "1", "page_count": "1", "text_length": "10"},
            {"run_id": "run-text", "competencia": "2026_02", "source_path": "b.pdf", "source_sha256": "sha-b", "page_number": "1", "page_count": "1", "text_length": "20"},
        ],
    )

    checker = make_checker(tmp_path, source, text, links)

    with pytest.raises(ValueError):
        checker.run()


def test_raw_artifact_consistency_fails_on_sha_lineage_mismatch(tmp_path: Path) -> None:
    source, text, links = create_valid_artifacts(tmp_path)

    write_csv(
        text,
        TEXT_FIELDS,
        [
            {"run_id": "run-text", "competencia": "2026_01", "source_path": "a.pdf", "source_sha256": "WRONG", "page_number": "1", "page_count": "1", "text_length": "10"},
            {"run_id": "run-text", "competencia": "2026_02", "source_path": "b.pdf", "source_sha256": "sha-b", "page_number": "1", "page_count": "1", "text_length": "20"},
        ],
    )

    checker = make_checker(tmp_path, source, text, links)

    with pytest.raises(ValueError):
        checker.run()
