from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ExecutionEvent:
    run_id: str
    event_id: str
    timestamp_utc: str
    layer: str
    module: str
    class_name: str
    method_name: str
    operation: str
    status: str
    severity: str
    message: str
    competencia: Optional[str] = None
    source_file: Optional[str] = None
    records_in: Optional[int] = None
    records_out: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None
    input_hash: Optional[str] = None
    output_hash: Optional[str] = None
    git_commit: Optional[str] = None
    environment: Optional[str] = None


class PipelineExecutionLogger:
    VALID_STATUS = {"STARTED", "SUCCESS", "WARNING", "FAILED", "SKIPPED"}
    VALID_SEVERITY = {"INFO", "WARNING", "ERROR", "CRITICAL"}

    def __init__(
        self,
        base_dir: str | Path = "data/raw/_audit/execution_log",
        run_id: Optional[str] = None,
        environment: Optional[str] = None,
        git_commit: Optional[str] = None,
        write_parquet: bool = True,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.run_id = run_id or self._new_run_id()
        self.environment = environment or os.getenv("FLORIANO_ENV", "local")
        self.git_commit = git_commit or self._detect_git_commit()
        self.write_parquet = write_parquet
        self.events: List[Dict[str, Any]] = []

        self.run_date = datetime.now(timezone.utc).date().isoformat()
        self.partition_dir = self.base_dir / f"run_date={self.run_date}"
        self.partition_dir.mkdir(parents=True, exist_ok=True)

        self.jsonl_path = self.partition_dir / f"part-{self.run_id}.jsonl"
        self.parquet_path = self.partition_dir / f"part-{self.run_id}.parquet"

    @staticmethod
    def _new_run_id() -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"run-{ts}-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def _detect_git_commit() -> Optional[str]:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                check=False,
            )
            value = result.stdout.strip()
            return value or None
        except Exception:
            return None

    def log(
        self,
        *,
        layer: str,
        module: str,
        class_name: str,
        method_name: str,
        operation: str,
        status: str,
        severity: str,
        message: str,
        competencia: Optional[str] = None,
        source_file: Optional[str] = None,
        records_in: Optional[int] = None,
        records_out: Optional[int] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None,
        input_hash: Optional[str] = None,
        output_hash: Optional[str] = None,
    ) -> Dict[str, Any]:
        if status not in self.VALID_STATUS:
            raise ValueError(f"Invalid status: {status}")

        if severity not in self.VALID_SEVERITY:
            raise ValueError(f"Invalid severity: {severity}")

        event = ExecutionEvent(
            run_id=self.run_id,
            event_id=uuid.uuid4().hex,
            timestamp_utc=self._utc_now(),
            layer=layer,
            module=module,
            class_name=class_name,
            method_name=method_name,
            operation=operation,
            status=status,
            severity=severity,
            message=message,
            competencia=competencia,
            source_file=source_file,
            records_in=records_in,
            records_out=records_out,
            error_type=error_type,
            error_message=error_message,
            duration_ms=duration_ms,
            input_hash=input_hash,
            output_hash=output_hash,
            git_commit=self.git_commit,
            environment=self.environment,
        )

        row = asdict(event)
        self.events.append(row)
        self._append_jsonl(row)
        return row

    def started(self, **kwargs: Any) -> Dict[str, Any]:
        return self.log(status="STARTED", severity="INFO", **kwargs)

    def success(self, **kwargs: Any) -> Dict[str, Any]:
        return self.log(status="SUCCESS", severity="INFO", **kwargs)

    def warning(self, **kwargs: Any) -> Dict[str, Any]:
        return self.log(status="WARNING", severity="WARNING", **kwargs)

    def failed(self, **kwargs: Any) -> Dict[str, Any]:
        return self.log(status="FAILED", severity="ERROR", **kwargs)

    def skipped(self, **kwargs: Any) -> Dict[str, Any]:
        return self.log(status="SKIPPED", severity="INFO", **kwargs)

    def _append_jsonl(self, row: Dict[str, Any]) -> None:
        with self.jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def flush_parquet(self, require_parquet: bool = False) -> Optional[Path]:
        if not self.write_parquet:
            return None

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            if require_parquet:
                raise RuntimeError(
                    "pyarrow não está instalado. Instale com: python -m pip install pyarrow"
                ) from exc
            return None

        table = pa.Table.from_pylist(self.events)
        pq.write_table(table, self.parquet_path)
        return self.parquet_path

    def close(self, require_parquet: bool = False) -> None:
        self.flush_parquet(require_parquet=require_parquet)


class ExecutionTimer:
    def __init__(self) -> None:
        self.started_at = time.perf_counter()

    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self.started_at) * 1000)
