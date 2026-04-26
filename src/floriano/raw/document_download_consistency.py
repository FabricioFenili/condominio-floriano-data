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
class DocumentDownloadConsistencyRecord:
    run_id: str
    check_name: str
    artifact_name: str
    severity: str
    status: str
    message: str
    details_json: str
    checked_at_utc: str


class DocumentDownloadConsistencyChecker:
    """
    Gate de consistência dos documentos baixados.

    Valida:
    - document_downloads_latest.csv;
    - run_id único;
    - competências compatíveis com pdf_links;
    - uri_sha256 compatível com links downloadarquivo;
    - ausência de falhas críticas;
    - conflito_detected = False;
    - saved_path existente;
    - file_sha256 compatível com arquivo físico;
    - file_size_bytes compatível com arquivo físico.

    Não interpreta conteúdo financeiro.
    """

    MODULE = "floriano.raw.document_download_consistency"
    CLASS_NAME = "DocumentDownloadConsistencyChecker"

    REQUIRED_DOWNLOAD_COLUMNS = {
        "run_id",
        "upstream_pdf_links_run_id",
        "competencia",
        "source_pdf_path",
        "source_pdf_sha256",
        "page_number",
        "uri",
        "uri_sha256",
        "link_type",
        "download_status",
        "http_status",
        "saved_path",
        "file_size_bytes",
        "file_sha256",
        "already_existed",
        "was_downloaded_now",
        "is_duplicate_uri",
        "is_duplicate_file",
        "conflict_detected",
        "error_type",
        "error_message",
    }

    REQUIRED_LINK_COLUMNS = {
        "competencia",
        "uri_sha256",
        "link_type",
    }

    ALLOWED_STATUSES = {
        "SUCCESS",
        "SKIPPED_EXISTING_URI",
        "DUPLICATE_FILE_SHA",
        "TARGET_EXISTS_SAME_HASH",
    }

    def __init__(
        self,
        document_downloads_path: str | Path = "data/raw/document_downloads/document_downloads_latest.csv",
        pdf_links_path: str | Path = "data/raw/pdf_links/pdf_links_latest.csv",
        output_dir: str | Path = "data/raw/quality",
        latest_registry_dir: str | Path = "data/raw/_latest",
        manifest_dir: str | Path = "data/raw/_manifests",
        logger: Optional[PipelineExecutionLogger] = None,
        require_parquet: bool = False,
    ) -> None:
        self.document_downloads_path = Path(document_downloads_path)
        self.pdf_links_path = Path(pdf_links_path)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.require_parquet = require_parquet

        self.registry = LatestArtifactRegistry(base_dir=latest_registry_dir)
        self.manifest_manager = ArtifactManifestManager(base_dir=manifest_dir)

        self.download_rows: List[Dict[str, str]] = []
        self.link_rows: List[Dict[str, str]] = []
        self.records: List[DocumentDownloadConsistencyRecord] = []

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
    def _load_csv(path: Path, required_columns: Set[str]) -> List[Dict[str, str]]:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = set(reader.fieldnames or [])

        missing = required_columns - fieldnames
        if missing:
            raise ValueError(f"{path} sem colunas obrigatórias: {sorted(missing)}")

        if not rows:
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
            DocumentDownloadConsistencyRecord(
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
            operation="load_document_download_artifacts",
            message="Carregando document_downloads e pdf_links.",
        )

        self.download_rows = self._load_csv(
            self.document_downloads_path,
            self.REQUIRED_DOWNLOAD_COLUMNS,
        )
        self.link_rows = self._load_csv(
            self.pdf_links_path,
            self.REQUIRED_LINK_COLUMNS,
        )

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_inputs",
            operation="load_document_download_artifacts",
            message="Artefatos carregados.",
            records_out=len(self.download_rows) + len(self.link_rows),
            duration_ms=timer.elapsed_ms(),
        )

    def check_single_run_id(self) -> None:
        run_ids = sorted({r["run_id"] for r in self.download_rows if r.get("run_id")})

        if len(run_ids) == 1:
            self._add(
                check_name="document_downloads_single_run_id",
                artifact_name="document_downloads",
                severity="INFO",
                status="PASS",
                message="document_downloads contém exatamente um run_id.",
                details={"run_ids": run_ids},
            )
        else:
            self._add(
                check_name="document_downloads_single_run_id",
                artifact_name="document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="document_downloads contém zero ou múltiplos run_id.",
                details={"run_ids": run_ids},
            )

    def check_scope_against_pdf_links(self) -> None:
        download_link_rows = [
            r for r in self.link_rows
            if r.get("link_type") == "downloadarquivo"
        ]

        allowed_competencias = {r["competencia"] for r in download_link_rows}
        allowed_uris = {r["uri_sha256"] for r in download_link_rows}

        download_competencias = {r["competencia"] for r in self.download_rows}
        download_uris = {r["uri_sha256"] for r in self.download_rows}

        unexpected_competencias = sorted(download_competencias - allowed_competencias)
        unexpected_uris = sorted(download_uris - allowed_uris)

        if unexpected_competencias or unexpected_uris:
            self._add(
                check_name="document_downloads_scope_against_pdf_links",
                artifact_name="document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="document_downloads contém competência ou uri_sha256 fora dos links downloadarquivo.",
                details={
                    "unexpected_competencias": unexpected_competencias,
                    "unexpected_uri_count": len(unexpected_uris),
                    "unexpected_uri_sample": unexpected_uris[:20],
                },
            )
        else:
            self._add(
                check_name="document_downloads_scope_against_pdf_links",
                artifact_name="document_downloads",
                severity="INFO",
                status="PASS",
                message="document_downloads está restrito aos links downloadarquivo extraídos.",
                details={
                    "download_competencias": sorted(download_competencias),
                    "download_uri_count": len(download_uris),
                },
            )

    def check_download_statuses(self) -> None:
        status_counter = Counter(r["download_status"] for r in self.download_rows)

        invalid_statuses = {
            status: count
            for status, count in status_counter.items()
            if status not in self.ALLOWED_STATUSES
        }

        if invalid_statuses:
            self._add(
                check_name="document_downloads_allowed_statuses",
                artifact_name="document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="document_downloads contém status crítico ou não permitido.",
                details={"invalid_statuses": invalid_statuses, "all_statuses": dict(status_counter)},
            )
        else:
            self._add(
                check_name="document_downloads_allowed_statuses",
                artifact_name="document_downloads",
                severity="INFO",
                status="PASS",
                message="document_downloads contém apenas status permitidos.",
                details={"all_statuses": dict(status_counter)},
            )

    def check_no_conflicts(self) -> None:
        conflicts = [
            r for r in self.download_rows
            if self._bool(r.get("conflict_detected", "False"))
        ]

        if conflicts:
            self._add(
                check_name="document_downloads_no_conflicts",
                artifact_name="document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="document_downloads contém conflitos.",
                details={
                    "conflict_count": len(conflicts),
                    "sample": [
                        {
                            "competencia": r["competencia"],
                            "uri_sha256": r["uri_sha256"],
                            "saved_path": r["saved_path"],
                            "status": r["download_status"],
                        }
                        for r in conflicts[:20]
                    ],
                },
            )
        else:
            self._add(
                check_name="document_downloads_no_conflicts",
                artifact_name="document_downloads",
                severity="INFO",
                status="PASS",
                message="document_downloads não contém conflitos.",
                details={},
            )

    def check_files_exist_and_hash(self) -> None:
        missing_path = []
        missing_file = []
        hash_mismatch = []
        size_mismatch = []
        empty_files = []

        unique_saved_paths = sorted({
            r["saved_path"]
            for r in self.download_rows
            if r.get("saved_path")
        })

        for r in self.download_rows:
            saved_path = r.get("saved_path", "")
            file_sha = r.get("file_sha256", "")
            expected_size = self._int(r.get("file_size_bytes", "0"))

            if not saved_path:
                missing_path.append(r)
                continue

            path = Path(saved_path)

            if not path.exists():
                missing_file.append(r)
                continue

            actual_size = path.stat().st_size
            actual_hash = sha256_file(path)

            if actual_size <= 0:
                empty_files.append(r)

            if expected_size != actual_size:
                size_mismatch.append(
                    {
                        "saved_path": saved_path,
                        "expected_size": expected_size,
                        "actual_size": actual_size,
                    }
                )

            if file_sha != actual_hash:
                hash_mismatch.append(
                    {
                        "saved_path": saved_path,
                        "expected_hash": file_sha,
                        "actual_hash": actual_hash,
                    }
                )

        if missing_path or missing_file or hash_mismatch or size_mismatch or empty_files:
            self._add(
                check_name="document_downloads_file_integrity",
                artifact_name="document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="Falha de integridade física dos arquivos baixados.",
                details={
                    "missing_path_count": len(missing_path),
                    "missing_file_count": len(missing_file),
                    "hash_mismatch_count": len(hash_mismatch),
                    "size_mismatch_count": len(size_mismatch),
                    "empty_files_count": len(empty_files),
                    "hash_mismatch_sample": hash_mismatch[:10],
                    "size_mismatch_sample": size_mismatch[:10],
                },
            )
        else:
            self._add(
                check_name="document_downloads_file_integrity",
                artifact_name="document_downloads",
                severity="INFO",
                status="PASS",
                message="Todos os saved_path existem e batem com file_sha256/file_size_bytes.",
                details={
                    "rows": len(self.download_rows),
                    "unique_saved_paths": len(unique_saved_paths),
                },
            )

    def check_duplicate_file_sha_allowed(self) -> None:
        duplicate_rows = [
            r for r in self.download_rows
            if r["download_status"] == "DUPLICATE_FILE_SHA"
        ]

        bad_duplicates = [
            r for r in duplicate_rows
            if not r.get("saved_path") or not r.get("file_sha256")
        ]

        if bad_duplicates:
            self._add(
                check_name="document_downloads_duplicate_file_sha_valid",
                artifact_name="document_downloads",
                severity="CRITICAL",
                status="FAIL",
                message="DUPLICATE_FILE_SHA sem saved_path ou file_sha256.",
                details={"bad_duplicate_count": len(bad_duplicates)},
            )
        else:
            self._add(
                check_name="document_downloads_duplicate_file_sha_valid",
                artifact_name="document_downloads",
                severity="INFO",
                status="PASS",
                message="DUPLICATE_FILE_SHA tratado como duplicidade permitida.",
                details={"duplicate_file_sha_count": len(duplicate_rows)},
            )

    def evaluate(self) -> Dict[str, object]:
        self.load_inputs()

        self.check_single_run_id()
        self.check_scope_against_pdf_links()
        self.check_download_statuses()
        self.check_no_conflicts()
        self.check_files_exist_and_hash()
        self.check_duplicate_file_sha_allowed()

        status_counts = Counter(r.status for r in self.records)
        severity_counts = Counter(r.severity for r in self.records)

        approved = status_counts.get("FAIL", 0) == 0

        return {
            "approved": approved,
            "status": "SUCCESS" if approved else "FAILED",
            "status_counts": dict(status_counts),
            "severity_counts": dict(severity_counts),
            "record_count": len(self.records),
            "download_record_count": len(self.download_rows),
            "unique_saved_path_count": len({r["saved_path"] for r in self.download_rows if r.get("saved_path")}),
            "status_distribution": dict(Counter(r["download_status"] for r in self.download_rows)),
        }

    def write_csv(self, rows: List[Dict[str, object]], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = list(DocumentDownloadConsistencyRecord.__dataclass_fields__.keys())

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return path

    def write_json(self, payload: Dict[str, object], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_parquet(self, rows: List[Dict[str, object]], path: Path) -> Optional[Path]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            if self.require_parquet:
                raise RuntimeError(
                    "pyarrow não está instalado. Instale com: python -m pip install pyarrow"
                ) from exc
            return None

        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)
        return path

    def write_output(self, summary: Dict[str, object]) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        rows = [asdict(record) for record in self.records]

        run_dir = self.output_dir / f"run_id={self.logger.run_id}"
        csv_path = run_dir / "document_download_consistency.csv"
        parquet_path = run_dir / "document_download_consistency.parquet"
        summary_path = run_dir / "document_download_consistency_summary.json"

        latest_csv_path = self.output_dir / "document_download_consistency_latest.csv"
        latest_summary_path = self.output_dir / "document_download_consistency_summary_latest.json"

        self.write_csv(rows, csv_path)
        self.write_csv(rows, latest_csv_path)
        parquet_written = self.write_parquet(rows, parquet_path)
        self.write_json(summary, summary_path)
        self.write_json(summary, latest_summary_path)

        manifest_path = self.manifest_manager.write_manifest(
            run_id=self.logger.run_id,
            module=self.MODULE,
            status=str(summary["status"]),
            input_artifacts=[
                {
                    "artifact_name": "document_downloads",
                    "path": str(self.document_downloads_path),
                    "hash": sha256_file(self.document_downloads_path),
                    "record_count": len(self.download_rows),
                },
                {
                    "artifact_name": "pdf_links",
                    "path": str(self.pdf_links_path),
                    "hash": sha256_file(self.pdf_links_path),
                    "record_count": len(self.link_rows),
                },
            ],
            output_artifacts=[
                {
                    "artifact_name": "document_download_consistency",
                    "path": str(csv_path),
                    "hash": sha256_file(csv_path),
                    "record_count": len(rows),
                },
                {
                    "artifact_name": "document_download_consistency_summary",
                    "path": str(summary_path),
                    "hash": sha256_file(summary_path),
                    "record_count": 1,
                },
            ],
            record_counts={
                "document_downloads": len(self.download_rows),
                "pdf_links": len(self.link_rows),
                "document_download_consistency": len(rows),
            },
            extra=summary,
        )

        if summary["status"] == "SUCCESS":
            run_ids = sorted({r["run_id"] for r in self.download_rows if r.get("run_id")})
            download_run_id = run_ids[0] if len(run_ids) == 1 else "unknown"

            self.registry.publish(
                artifact_name="document_downloads",
                run_id=download_run_id,
                path=self.document_downloads_path,
                record_count=len(self.download_rows),
                status="SUCCESS",
                module="floriano.raw.direct_document_downloader",
                manifest_path=str(manifest_path),
            )

            self.registry.publish(
                artifact_name="document_download_consistency",
                run_id=self.logger.run_id,
                path=latest_csv_path,
                record_count=len(rows),
                status="SUCCESS",
                module=self.MODULE,
                manifest_path=str(manifest_path),
            )

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="write_output",
            operation="write_document_download_consistency",
            message="Relatório de consistência dos downloads gravado.",
            records_out=len(rows),
            duration_ms=timer.elapsed_ms(),
            source_file=str(csv_path),
        )

        return {
            "csv_path": str(csv_path),
            "latest_csv_path": str(latest_csv_path),
            "parquet_path": str(parquet_written) if parquet_written else None,
            "summary_path": str(summary_path),
            "latest_summary_path": str(latest_summary_path),
            "manifest_path": str(manifest_path),
        }

    def run(self) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="run",
            operation="document_download_consistency_pipeline",
            message="Gate de consistência dos documentos baixados iniciado.",
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
                    operation="document_download_consistency_pipeline",
                    message="Gate de consistência dos documentos baixados reprovado.",
                    duration_ms=timer.elapsed_ms(),
                )
                self.logger.close(require_parquet=False)
                raise ValueError("Gate de consistência dos documentos baixados reprovado.")

            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="document_download_consistency_pipeline",
                message="Gate de consistência dos documentos baixados aprovado.",
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
                operation="document_download_consistency_pipeline",
                message="Gate de consistência dos documentos baixados falhou.",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                duration_ms=timer.elapsed_ms(),
            )
            self.logger.close(require_parquet=False)
            raise
