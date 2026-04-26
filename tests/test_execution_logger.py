from __future__ import annotations
import json
from pathlib import Path

import pytest

from floriano.utils.execution_logger import PipelineExecutionLogger


def test_execution_logger_writes_jsonl(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    event = logger.success(
        layer="raw",
        module="test.module",
        class_name="TestClass",
        method_name="test_method",
        operation="unit_test",
        message="Evento de teste.",
    )

    assert event["run_id"] == "run-test"
    assert logger.jsonl_path.exists()

    rows = [
        json.loads(line)
        for line in logger.jsonl_path.read_text(encoding="utf-8").splitlines()
    ]

    assert len(rows) == 1
    assert rows[0]["status"] == "SUCCESS"
    assert rows[0]["severity"] == "INFO"


def test_execution_logger_rejects_invalid_status(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    with pytest.raises(ValueError):
        logger.log(
            layer="raw",
            module="test.module",
            class_name="TestClass",
            method_name="test_method",
            operation="unit_test",
            status="INVALID",
            severity="INFO",
            message="Evento inválido.",
        )


def test_execution_logger_flush_parquet_optional(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    logger.success(
        layer="raw",
        module="test.module",
        class_name="TestClass",
        method_name="test_method",
        operation="unit_test",
        message="Evento de teste.",
    )

    result = logger.flush_parquet(require_parquet=False)

    assert result is None or result.exists()
