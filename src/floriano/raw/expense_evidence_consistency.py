from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

from floriano.utils.artifacts import (
    ArtifactManifestManager,
    LatestArtifactRegistry,
    sha256_file,
)
from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class ExpenseEvidenceConsistencyRecord:
    run_id: str
    check_name: str
    artifact_name: str
    severity: str
    status: str
    message: str
    details_json: str
    checked_at_utc: str


class ExpenseEvidenceConsistencyChecker:
    """
    Gate de consistência do fluxo RAW de despesas.

    Valida:
    - expense_pages_latest.csv;
    - expense_documents_discovered_latest.csv;
    - expense_document_downloads_latest.csv;
    - versionamento de expense_id / page_version_id / document_id;
    - snapshot HTML físico e hash;
    - documentos físicos e hash;
    - conflitos;
    - status críticos.
    """

    MODULE = "floriano.raw.expense_evidence_consistency"
    CLASS_NAME = "ExpenseEvidenceConsistencyChecker"

    PAGE_REQUIRED = {
        "run_id",
        "competencia",
        "expense_id",
        "expense_uri",
        "expense_uri_sha256",
        "final_url",
        "http_status",
        "content_type",
        "is_pdf_direct",
        "is_html_page",
        "html_saved_path",
        "html_sha256",
        "expense_page_version_id",
        "document_links_found",
        "crawl_status",
    }

    DOC_REQUIRED = {
        "run_id",
        "expense_id",
        "expense_page_version_id",
        "competencia",
        "document_id",
        "document_uri",
        "document_uri_sha256",
        "document_link_source",
    }

    DOWNLOAD_REQUIRED = {
        "run_id",
        "expense_id",
        "expense_page_version_id",
        "document_id",
        "competencia",
        "document_uri",
        "document_uri_sha256",
        "download_status",
        "saved_path",
        "file_size_bytes",
        "file_sha256",
        "conflict_detected",
    }

    ALLOWED_CRAWL_STATUSES = {
        "DIRECT_PDF_DISCOVERED",
        "DIRECT_IMAGE_DISCOVERED",
        "DIRECT_BINARY_DISCOVERED",
        "HTML_PAGE_CRAWLED",
        "DRY_RUN",
    }

    ALLOWED_DOWNLOAD_STATUSES = {
        "SUCCESS",
        "SKIPPED_EXISTING_DOCUMENT",
        "DUPLICATE_FILE_SHA",
        "TARGET_EXISTS_SAME_HASH",
        "DRY_RUN",
    }

    def __init__(
        self,
        expense_pages_path: str | Path = "data/raw/expense_pages/expense_pages_latest.csv",
        expense_documents_path: str | Path = "data/raw/expense_pages/expense_documents_discovered_latest.csv",
        expense_downloads_path: str | Path = "data/raw/expense_document_downloads/expense_document_downloads_latest.csv",
        output_dir: str | Path = "data/raw/quality",
        latest_registry_dir: str | Path = "data/raw/_latest",
        manifest_dir: str | Path = "data/raw/_manifests",
        logger: Optional[PipelineExecutionLogger] = None,
        require_parquet: bool = False,
        allow_empty_downloads: bool = False,
    ) -> None:
        self.expense_pages_path = Path(expense_pages_path)
        self.expense_documents_path = Path(expense_documents_path)
        self.expense_downloads_path = Path(expense_downloads_path)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.require_parquet = require_parquet
        self.allow_empty_downloads = allow_empty_downloads

        self.registry = LatestArtifactRegistry(base_dir=latest_registry_dir)
        self.manifest_manager = ArtifactManifestManager(base_dir=manifest_dir)

        self.page_rows: List[Dict[str, str]] = []
        self.doc_rows: List[Dict[str, str]] = []
        self.download_rows: List[Dict[str, str]] = []
        self.records: List[ExpenseEvidenceConsistencyRecord] = []

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def _bool(value: str) -> bool:
        return str(value).strip().lower() == "true"

    @staticmethod
    def _int(value: str) -> int:
        if value is None or value == "":
            return 0
        return int(float(value))

    @staticmethod
    def _load_csv(path: Path, required: Set[str], *, allow_empty: bool = False) -> List[Dict[str, str]]:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = set(reader.fieldnames or [])

        missing = required - fieldnames
        if missing:
            raise ValueError(f"{path} sem colunas obrigatórias: {sorted(missing)}")

        if not rows and not allow_empty:
            raise ValueError(f"{path} está vazio.")

        return rows

    def _add(
        self,
        *,
        check_name: str,
        artifact_name: str,
        severity: str,
        status: str,
        message: str,
        details: Optional[Dict[str, object]] = None,
    ) -> None:
        self.records.append(
            ExpenseEvidenceConsistencyRecord(
                run_id=self.logger.run_id,
                check_name=check_name,
                artifact_name=artifact_name,
                severity=severity,
                status=status,
                message=message,
                details_json=json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                checked_at_utc=self._utc_now(),
            )
        )

    def load_inputs(self) -> None:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_inputs",
            operation="load_expense_evidence_artifacts",
            message="Carregando artefatos RAW de despesas.",
        )

        self.page_rows = self._load_csv(self.expense_pages_path, self.PAGE_REQUIRED)
        self.doc_rows = self._load_csv(self.expense_documents_path, self.DOC_REQUIRED, allow_empty=True)

        if self.expense_downloads_path.exists():
            self.download_rows = self._load_csv(
                self.expense_downloads_path,
                self.DOWNLOAD_REQUIRED,
                allow_empty=self.allow_empty_downloads,
            )
        else:
            self.download_rows = []

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_inputs",
            operation="load_expense_evidence_artifacts",
            message="Artefatos RAW de despesas carregados.",
            records_out=len(self.page_rows) + len(self.doc_rows) + len(self.download_rows),
            duration_ms=timer.elapsed_ms(),
        )

    def check_single_run_ids(self) -> None:
        artifacts = [
            ("expense_pages", self.page_rows),
            ("expense_documents_discovered", self.doc_rows),
            ("expense_document_downloads", self.download_rows),
        ]

        for name, rows in artifacts:
            if not rows:
                self._add(
                    check_name="single_run_id",
                    artifact_name=name,
                    severity="WARNING",
                    status="WARN",
                    message=f"{name} vazio; sem run_id a validar.",
                    details={},
                )
                continue

            run_ids = sorted({r["run_id"] for r in rows if r.get("run_id")})

            if len(run_ids) == 1:
                self._add(
                    check_name="single_run_id",
                    artifact_name=name,
                    severity="INFO",
                    status="PASS",
                    message=f"{name} contém exatamente um run_id.",
                    details={"run_ids": run_ids},
                )
            else:
                self._add(
                    check_name="single_run_id",
                    artifact_name=name,
                    severity="CRITICAL",
                    status="FAIL",
                    message=f"{name} contém zero ou múltiplos run_id.",
                    details={"run_ids": run_ids},
                )

    def check_page_statuses(self) -> None:
        status_counter = Counter(r["crawl_status"] for r in self.page_rows)
        invalid = {k: v for k, v in status_counter.items() if k not in self.ALLOWED_CRAWL_STATUSES}

        if invalid:
            self._add(
                check_name="expense_pages_allowed_statuses",
                artifact_name="expense_pages",
                severity="CRITICAL",
                status="FAIL",
                message="expense_pages contém crawl_status crítico ou não permitido.",
                details={"invalid": invalid, "all_statuses": dict(status_counter)},
            )
        else:
            self._add(
                check_name="expense_pages_allowed_statuses",
                artifact_name="expense_pages",
                severity="INFO",
                status="PASS",
                message="expense_pages contém apenas crawl_status permitidos.",
                details={"all_statuses": dict(status_counter)},
            )

    def check_html_snapshots(self) -> None:
        html_pages = [r for r in self.page_rows if self._bool(r.get("is_html_page", "False"))]

        missing = []
        hash_mismatch = []
        missing_version = []

        for r in html_pages:
            if not r.get("expense_page_version_id"):
                missing_version.append(r["expense_id"])

            path_str = r.get("html_saved_path", "")
            expected_hash = r.get("html_sha256", "")

            if not path_str:
                missing.append(r["expense_id"])
                continue

            path = Path(path_str)

            if not path.exists():
                missing.append(r["expense_id"])
                continue

            actual_hash = sha256_file(path)

            if actual_hash != expected_hash:
                hash_mismatch.append(r["expense_id"])

        if missing or hash_mismatch or missing_version:
            self._add(
                check_name="expense_pages_html_snapshot_integrity",
                artifact_name="expense_pages",
                severity="CRITICAL",
                status="FAIL",
                message="Falha de integridade em snapshots HTML de despesas.",
                details={
                    "missing_count": len(missing),
                    "hash_mismatch_count": len(hash_mismatch),
                    "missing_version_count": len(missing_version),
                    "missing_sample": missing[:20],
                    "hash_mismatch_sample": hash_mismatch[:20],
                },
            )
        else:
            self._add(
                check_name="expense_pages_html_snapshot_integrity",
                artifact_name="expense_pages",
                severity="INFO",
                status="PASS",
                message="Snapshots HTML versionados existem e hashes conferem.",
                details={"html_pages": len(html_pages)},
            )

    def check_document_discovery_scope(self) -> None:
        page_expense_ids = {r["expense_id"] for r in self.page_rows}
        doc_expense_ids = {r["expense_id"] for r in self.doc_rows}

        unexpected = sorted(doc_expense_ids - page_expense_ids)

        doc_id_counter = Counter(r["document_id"] for r in self.doc_rows)
        duplicate_doc_ids = sorted([k for k, v in doc_id_counter.items() if v > 1])

        if unexpected or duplicate_doc_ids:
            self._add(
                check_name="expense_documents_discovery_scope",
                artifact_name="expense_documents_discovered",
                severity="CRITICAL",
                status="FAIL",
                message="Documentos descobertos fora do escopo de páginas ou com document_id duplicado.",
                details={
                    "unexpected_expense_ids": unexpected[:20],
                    "duplicate_document_id_count": len(duplicate_doc_ids),
                    "duplicate_document_id_sample": duplicate_doc_ids[:20],
                },
            )
        else:
            self._add(
                check_name="expense_documents_discovery_scope",
                artifact_name="expense_documents_discovered",
                severity="INFO",
                status="PASS",
                message="Documentos descobertos estão no escopo de expense_pages e sem document_id duplicado.",
                details={"document_count": len(self.doc_rows)},
            )

    def check_download_scope_status_and_files(self) -> None:
        if not self.download_rows:
            if self.doc_rows:
                self._add(
                    check_name="expense_downloads_presence",
                    artifact_name="expense_document_downloads",
                    severity="CRITICAL",
                    status="FAIL",
                    message="Há documentos descobertos, mas não há downloads registrados.",
                    details={"document_count": len(self.doc_rows)},
                )
            else:
                self._add(
                    check_name="expense_downloads_presence",
                    artifact_name="expense_document_downloads",
                    severity="WARNING",
                    status="WARN",
                    message="Nenhum documento descoberto/baixado no fluxo de despesas.",
                    details={},
                )
            return

        discovered_ids = {r["document_id"] for r in self.doc_rows}
        downloaded_ids = {r["document_id"] for r in self.download_rows}

        missing_downloads = sorted(discovered_ids - downloaded_ids)
        unexpected_downloads = sorted(downloaded_ids - discovered_ids)

        status_counter = Counter(r["download_status"] for r in self.download_rows)
        invalid_statuses = {k: v for k, v in status_counter.items() if k not in self.ALLOWED_DOWNLOAD_STATUSES}

        conflicts = [r for r in self.download_rows if self._bool(r.get("conflict_detected", "False"))]

        missing_files = []
        hash_mismatch = []
        size_mismatch = []

        for r in self.download_rows:
            status = r["download_status"]

            if status == "DRY_RUN":
                continue

            saved_path = r.get("saved_path", "")
            if not saved_path:
                missing_files.append(r["document_id"])
                continue

            path = Path(saved_path)
            if not path.exists():
                missing_files.append(r["document_id"])
                continue

            expected_hash = r.get("file_sha256", "")
            expected_size = self._int(r.get("file_size_bytes", "0"))
            actual_hash = sha256_file(path)
            actual_size = path.stat().st_size

            if expected_hash != actual_hash:
                hash_mismatch.append(r["document_id"])

            if expected_size != actual_size:
                size_mismatch.append(r["document_id"])

        if (
            missing_downloads
            or unexpected_downloads
            or invalid_statuses
            or conflicts
            or missing_files
            or hash_mismatch
            or size_mismatch
        ):
            self._add(
                check_name="expense_downloads_integrity",
                artifact_name="expense_document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="Falha de escopo, status ou integridade física dos documentos de despesas.",
                details={
                    "missing_download_count": len(missing_downloads),
                    "unexpected_download_count": len(unexpected_downloads),
                    "invalid_statuses": invalid_statuses,
                    "conflict_count": len(conflicts),
                    "missing_file_count": len(missing_files),
                    "hash_mismatch_count": len(hash_mismatch),
                    "size_mismatch_count": len(size_mismatch),
                    "status_distribution": dict(status_counter),
                },
            )
        else:
            self._add(
                check_name="expense_downloads_integrity",
                artifact_name="expense_document_downloads",
                severity="INFO",
                status="PASS",
                message="Downloads de despesas estão íntegros, sem conflitos e dentro do escopo.",
                details={
                    "download_count": len(self.download_rows),
                    "unique_document_ids": len(downloaded_ids),
                    "status_distribution": dict(status_counter),
                },
            )

    def evaluate(self) -> Dict[str, object]:
        self.load_inputs()

        self.check_single_run_ids()
        self.check_page_statuses()
        self.check_html_snapshots()
        self.check_document_discovery_scope()
        self.check_download_scope_status_and_files()

        status_counts = Counter(r.status for r in self.records)
        severity_counts = Counter(r.severity for r in self.records)

        approved = status_counts.get("FAIL", 0) == 0

        return {
            "approved": approved,
            "status": "SUCCESS" if approved else "FAILED",
            "status_counts": dict(status_counts),
            "severity_counts": dict(severity_counts),
            "record_count": len(self.records),
            "expense_pages_count": len(self.page_rows),
            "expense_documents_discovered_count": len(self.doc_rows),
            "expense_document_downloads_count": len(self.download_rows),
        }

    def write_csv(self, rows: List[Dict[str, object]], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        fields = list(ExpenseEvidenceConsistencyRecord.__dataclass_fields__.keys())

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

        return path

    @staticmethod
    def write_json(payload: Dict[str, object], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_parquet(self, rows: List[Dict[str, object]], path: Path) -> Optional[Path]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            if self.require_parquet:
                raise RuntimeError("pyarrow não está instalado.") from exc
            return None

        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)
        return path

    def write_output(self, summary: Dict[str, object]) -> Dict[str, Optional[str]]:
        rows = [asdict(r) for r in self.records]

        run_dir = self.output_dir / f"run_id={self.logger.run_id}"
        csv_path = run_dir / "expense_evidence_consistency.csv"
        parquet_path = run_dir / "expense_evidence_consistency.parquet"
        summary_path = run_dir / "expense_evidence_consistency_summary.json"

        latest_csv = self.output_dir / "expense_evidence_consistency_latest.csv"
        latest_summary = self.output_dir / "expense_evidence_consistency_summary_latest.json"

        self.write_csv(rows, csv_path)
        self.write_csv(rows, latest_csv)
        parquet_written = self.write_parquet(rows, parquet_path)
        self.write_json(summary, summary_path)
        self.write_json(summary, latest_summary)

        manifest_path = self.manifest_manager.write_manifest(
            run_id=self.logger.run_id,
            module=self.MODULE,
            status=str(summary["status"]),
            input_artifacts=[
                {
                    "artifact_name": "expense_pages",
                    "path": str(self.expense_pages_path),
                    "hash": sha256_file(self.expense_pages_path),
                    "record_count": len(self.page_rows),
                },
                {
                    "artifact_name": "expense_documents_discovered",
                    "path": str(self.expense_documents_path),
                    "hash": sha256_file(self.expense_documents_path),
                    "record_count": len(self.doc_rows),
                },
                {
                    "artifact_name": "expense_document_downloads",
                    "path": str(self.expense_downloads_path),
                    "hash": sha256_file(self.expense_downloads_path) if self.expense_downloads_path.exists() else None,
                    "record_count": len(self.download_rows),
                },
            ],
            output_artifacts=[
                {
                    "artifact_name": "expense_evidence_consistency",
                    "path": str(csv_path),
                    "hash": sha256_file(csv_path),
                    "record_count": len(rows),
                },
                {
                    "artifact_name": "expense_evidence_consistency_summary",
                    "path": str(summary_path),
                    "hash": sha256_file(summary_path),
                    "record_count": 1,
                },
            ],
            record_counts={
                "expense_pages": len(self.page_rows),
                "expense_documents_discovered": len(self.doc_rows),
                "expense_document_downloads": len(self.download_rows),
                "expense_evidence_consistency": len(rows),
            },
            extra=summary,
        )

        if summary["status"] == "SUCCESS":
            self.registry.publish(
                artifact_name="expense_evidence_consistency",
                run_id=self.logger.run_id,
                path=latest_csv,
                record_count=len(rows),
                status="SUCCESS",
                module=self.MODULE,
                manifest_path=str(manifest_path),
            )

        return {
            "csv_path": str(csv_path),
            "latest_csv_path": str(latest_csv),
            "parquet_path": str(parquet_written) if parquet_written else None,
            "summary_path": str(summary_path),
            "latest_summary_path": str(latest_summary),
            "manifest_path": str(manifest_path),
        }

    def run(self) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="run",
            operation="expense_evidence_consistency_pipeline",
            message="Gate de consistência das evidências de despesas iniciado.",
        )

        try:
            summary = self.evaluate()
            outputs = self.write_output(summary)

            if not summary["approved"]:
                self.logger.failed(
                    layer="raw",
                    module=self.MODULE,
                    class_name=self.CLASS_NAME,
                    method_name="run",
                    operation="expense_evidence_consistency_pipeline",
                    message="Gate de evidências de despesas reprovado.",
                    duration_ms=timer.elapsed_ms(),
                )
                self.logger.close(require_parquet=False)
                raise ValueError("Gate de evidências de despesas reprovado.")

            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="expense_evidence_consistency_pipeline",
                message="Gate de evidências de despesas aprovado.",
                records_out=len(self.records),
                duration_ms=timer.elapsed_ms(),
            )

            self.logger.close(require_parquet=self.require_parquet)
            return outputs

        except Exception as exc:
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="expense_evidence_consistency_pipeline",
                message="Gate de evidências de despesas falhou.",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                duration_ms=timer.elapsed_ms(),
            )
            self.logger.close(require_parquet=False)
            raise
