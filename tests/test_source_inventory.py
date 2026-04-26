from __future__ import annotations
import csv
import hashlib
from pathlib import Path

import pytest

from floriano.raw.source_inventory import SourceInventoryBuilder
from floriano.utils.execution_logger import PipelineExecutionLogger


def write_fake_pdf(path: Path, content: bytes = b"%PDF-1.7 fake\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_build_month_range() -> None:
    months = SourceInventoryBuilder.build_month_range("2025_11", "2026_02")
    assert months == ["2025_11", "2025_12", "2026_01", "2026_02"]


def test_source_inventory_success(tmp_path: Path) -> None:
    manual_upload = tmp_path / "manual_upload"

    write_fake_pdf(
        manual_upload / "2025_05" / "source" / "prestacao_contas_2025_05_print.pdf",
        b"abc",
    )
    write_fake_pdf(
        manual_upload / "2025_06" / "source" / "prestacao_contas_2025_06_print.pdf",
        b"def",
    )

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=manual_upload,
        output_dir=tmp_path / "raw" / "source_inventory",
        expected_months=["2025_05", "2025_06"],
        logger=logger,
        strict=True,
    )

    outputs = builder.run()

    latest_csv = Path(outputs["latest_csv_path"])
    assert latest_csv.exists()

    with latest_csv.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    assert rows[0]["competencia"] == "2025_05"
    assert rows[0]["validation_status"] == "VALID"
    assert rows[0]["sha256"] == hashlib.sha256(b"abc").hexdigest()


def test_source_inventory_missing_file_fails_in_strict_mode(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=tmp_path / "manual_upload",
        output_dir=tmp_path / "raw" / "source_inventory",
        expected_months=["2025_05"],
        logger=logger,
        strict=True,
    )

    with pytest.raises(ValueError):
        builder.run()


def test_source_inventory_missing_file_allowed_in_non_strict_mode(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=tmp_path / "manual_upload",
        output_dir=tmp_path / "raw" / "source_inventory",
        expected_months=["2025_05"],
        logger=logger,
        strict=False,
    )

    outputs = builder.run()
    latest_csv = Path(outputs["latest_csv_path"])

    with latest_csv.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert rows[0]["validation_status"] == "MISSING"


def test_source_inventory_empty_file_invalid(tmp_path: Path) -> None:
    manual_upload = tmp_path / "manual_upload"
    empty_file = manual_upload / "2025_05" / "source" / "prestacao_contas_2025_05_print.pdf"
    empty_file.parent.mkdir(parents=True, exist_ok=True)
    empty_file.touch()

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=manual_upload,
        output_dir=tmp_path / "raw" / "source_inventory",
        expected_months=["2025_05"],
        logger=logger,
        strict=False,
    )

    rows = builder.build_inventory()

    assert rows[0]["validation_status"] == "EMPTY"
    assert rows[0]["is_empty"] is True


def test_source_inventory_default_range_uses_m_minus_one(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=tmp_path / "manual_upload",
        output_dir=tmp_path / "raw" / "source_inventory",
        logger=logger,
        strict=False,
        as_of_date="2026-04-26",
    )

    assert builder.expected_months[0] == "2025_05"
    assert builder.expected_months[-1] == "2026_03"
    assert "2026_04" not in builder.expected_months


def test_source_inventory_single_month_override(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=tmp_path / "manual_upload",
        output_dir=tmp_path / "raw" / "source_inventory",
        month="2026_04",
        logger=logger,
        strict=False,
        as_of_date="2026-04-26",
    )

    assert builder.expected_months == ["2026_04"]


def test_source_inventory_explicit_range_override(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    builder = SourceInventoryBuilder(
        manual_upload_dir=tmp_path / "manual_upload",
        output_dir=tmp_path / "raw" / "source_inventory",
        start_month="2026_02",
        end_month="2026_03",
        logger=logger,
        strict=False,
        as_of_date="2026-04-26",
    )

    assert builder.expected_months == ["2026_02", "2026_03"]
