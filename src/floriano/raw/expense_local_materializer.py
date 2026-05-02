from __future__ import annotations

import csv
import hashlib
import json
import mimetypes
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class MaterializationResult:
    total_rows: int
    local_rows: int
    copied_rows: int
    downloaded_rows: int
    skipped_existing_rows: int
    conflict_rows: int
    missing_rows: int
    manifest_count: int


class ExpenseLocalMaterializer:
    """
    Pós-processador RAW de despesas.

    Garante que todo documento descoberto/baixado de despesa tenha arquivo físico
    dentro da própria pasta da despesa, ainda que o conteúdo já exista em outro
    caminho por file_sha256.

    Regra operacional:
    - saved_path final deve apontar para:
      data/landing/downloads/YYYY_MM/despesas/expense_id=<ID>/documentos/<document_id>.<ext>
    - cada expense_id deve ter manifest/expense_evidence_manifest.json.
    - não sobrescreve arquivo local com hash divergente.
    """

    MODULE = "floriano.raw.expense_local_materializer"
    CLASS_NAME = "ExpenseLocalMaterializer"

    DOWNLOAD_REQUIRED = {
        "run_id",
        "expense_id",
        "expense_page_version_id",
        "document_id",
        "competencia",
        "document_uri",
        "document_uri_sha256",
        "download_status",
        "content_type",
        "saved_path",
        "file_size_bytes",
        "file_sha256",
        "conflict_detected",
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
        "document_type_hint",
    }

    PAGE_REQUIRED = {
        "run_id",
        "competencia",
        "expense_id",
        "expense_uri",
        "expense_uri_sha256",
        "final_url",
        "final_url_sha256",
        "html_saved_path",
        "html_sha256",
        "expense_page_version_id",
    }

    EXPECTED_CONTENT_PREFIXES = (
        "application/pdf",
        "image/",
        "application/octet-stream",
    )

    def __init__(
        self,
        expense_downloads_path: str | Path = "data/raw/expense_document_downloads/expense_document_downloads_latest.csv",
        expense_documents_path: str | Path = "data/raw/expense_pages/expense_documents_discovered_latest.csv",
        expense_pages_path: str | Path = "data/raw/expense_pages/expense_pages_latest.csv",
        landing_downloads_dir: str | Path = "data/landing/downloads",
        output_dir: str | Path = "data/raw/expense_document_downloads",
        logger: Optional[PipelineExecutionLogger] = None,
        timeout_seconds: int = 60,
        require_parquet: bool = False,
    ) -> None:
        self.expense_downloads_path = Path(expense_downloads_path)
        self.expense_documents_path = Path(expense_documents_path)
        self.expense_pages_path = Path(expense_pages_path)
        self.landing_downloads_dir = Path(landing_downloads_dir)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.timeout_seconds = timeout_seconds
        self.require_parquet = require_parquet

        self.download_rows: List[Dict[str, str]] = []
        self.document_rows: List[Dict[str, str]] = []
        self.page_rows: List[Dict[str, str]] = []
        self.materialized_rows: List[Dict[str, str]] = []
        self.result: Optional[MaterializationResult] = None

    @staticmethod
    def utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def sha256_bytes(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def sha256_file(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def normalize_content_type(content_type: Optional[str]) -> str:
        if not content_type:
            return ""
        return content_type.lower().split(";", 1)[0].strip()

    @classmethod
    def is_expected_content_type(cls, content_type: Optional[str]) -> bool:
        clean = cls.normalize_content_type(content_type)
        return any(clean.startswith(prefix) for prefix in cls.EXPECTED_CONTENT_PREFIXES)

    @staticmethod
    def extension_from_content_type(content_type: Optional[str]) -> str:
        clean = (content_type or "").lower().split(";", 1)[0].strip()
        mapping = {
            "application/pdf": ".pdf",
            "image/jpeg": ".jpg",
            "image/jpg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
            "image/gif": ".gif",
            "application/octet-stream": ".bin",
        }
        if clean in mapping:
            return mapping[clean]
        return mimetypes.guess_extension(clean) or ".bin"

    @staticmethod
    def load_csv(path: Path, required: set[str], allow_empty: bool = False) -> List[Dict[str, str]]:
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
            raise ValueError(f"{path} vazio")

        return rows

    @staticmethod
    def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def write_parquet(self, path: Path, rows: List[Dict[str, str]]) -> Optional[Path]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            if self.require_parquet:
                raise RuntimeError("pyarrow não instalado") from exc
            return None

        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)
        return path

    def load_inputs(self) -> None:
        self.download_rows = self.load_csv(self.expense_downloads_path, self.DOWNLOAD_REQUIRED)
        self.document_rows = self.load_csv(self.expense_documents_path, self.DOC_REQUIRED, allow_empty=True)
        self.page_rows = self.load_csv(self.expense_pages_path, self.PAGE_REQUIRED, allow_empty=True)

    def local_document_dir(self, competencia: str, expense_id: str) -> Path:
        return self.landing_downloads_dir / competencia / "despesas" / f"expense_id={expense_id}" / "documentos"

    def local_manifest_path(self, competencia: str, expense_id: str) -> Path:
        return self.landing_downloads_dir / competencia / "despesas" / f"expense_id={expense_id}" / "manifest" / "expense_evidence_manifest.json"

    def local_path_for(self, row: Dict[str, str], content_type: Optional[str], source_path: Optional[Path] = None) -> Path:
        competencia = row["competencia"]
        expense_id = row["expense_id"]
        document_id = row["document_id"]
        ext = self.extension_from_content_type(content_type)

        if ext == ".bin" and source_path is not None and source_path.suffix:
            ext = source_path.suffix.lower()

        folder = self.local_document_dir(competencia, expense_id)
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{document_id}{ext}"

    def fetch_document(self, uri: str) -> tuple[bytes, Optional[str], Optional[int]]:
        import requests

        response = requests.get(uri, timeout=self.timeout_seconds)
        status = int(response.status_code or 0)
        content_type = response.headers.get("content-type") or response.headers.get("Content-Type")

        if status < 200 or status >= 300:
            raise RuntimeError(f"HTTP status {status}")

        content = bytes(response.content or b"")
        if not content:
            raise RuntimeError("Resposta sem conteúdo")

        if not self.is_expected_content_type(content_type):
            raise RuntimeError(f"Content-Type inesperado: {content_type}")

        return content, content_type, status

    def materialize_row(self, row: Dict[str, str]) -> Dict[str, str]:
        updated = dict(row)
        competencia = row["competencia"]
        expense_id = row["expense_id"]
        document_id = row["document_id"]
        current_saved_path = row.get("saved_path") or ""
        current_content_type = row.get("content_type") or None
        expected_hash = row.get("file_sha256") or ""

        source_path = Path(current_saved_path) if current_saved_path else None
        source_exists = bool(source_path and source_path.exists())

        target = self.local_path_for(row, current_content_type, source_path if source_exists else None)

        if target.exists():
            actual_hash = self.sha256_file(target)
            actual_size = target.stat().st_size

            if expected_hash and actual_hash != expected_hash:
                updated["download_status"] = "TARGET_EXISTS_CONFLICT"
                updated["conflict_detected"] = "True"
                updated["error_type"] = "LocalHashConflict"
                updated["error_message"] = "Arquivo local existe com hash diferente do registro."
                updated["saved_path"] = str(target)
                updated["file_sha256"] = actual_hash
                updated["file_size_bytes"] = str(actual_size)
                return updated

            updated["download_status"] = "SKIPPED_EXISTING_DOCUMENT"
            updated["saved_path"] = str(target)
            updated["file_sha256"] = actual_hash
            updated["file_size_bytes"] = str(actual_size)
            updated["already_existed"] = "True"
            updated["was_downloaded_now"] = "False"
            updated["conflict_detected"] = "False"
            updated["error_type"] = ""
            updated["error_message"] = ""
            return updated

        if source_exists:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target)
            actual_hash = self.sha256_file(target)
            actual_size = target.stat().st_size
            copied_from_existing = True
            downloaded_now = False
        else:
            content, fetched_content_type, http_status = self.fetch_document(row["document_uri"])
            if not current_content_type:
                current_content_type = fetched_content_type
                target = self.local_path_for(row, current_content_type, None)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            actual_hash = self.sha256_file(target)
            actual_size = target.stat().st_size
            updated["http_status"] = str(http_status)
            updated["content_type"] = fetched_content_type or ""
            copied_from_existing = False
            downloaded_now = True

        if expected_hash and actual_hash != expected_hash:
            updated["download_status"] = "TARGET_EXISTS_CONFLICT"
            updated["conflict_detected"] = "True"
            updated["error_type"] = "MaterializationHashMismatch"
            updated["error_message"] = "Hash materializado diverge do hash esperado."
        else:
            original_status = row.get("download_status") or "SUCCESS"
            if original_status in {"DUPLICATE_FILE_SHA", "SUCCESS", "TARGET_EXISTS_SAME_HASH", "SKIPPED_EXISTING_DOCUMENT"}:
                updated["download_status"] = "DUPLICATE_FILE_SHA" if row.get("is_duplicate_file") == "True" else "SUCCESS"
            updated["conflict_detected"] = "False"
            updated["error_type"] = ""
            updated["error_message"] = ""

        updated["saved_path"] = str(target)
        updated["file_sha256"] = actual_hash
        updated["file_size_bytes"] = str(actual_size)
        updated["already_existed"] = "False" if not copied_from_existing else "True"
        updated["was_downloaded_now"] = "True" if (downloaded_now or copied_from_existing) else "False"
        return updated

    def write_local_manifests(self) -> int:
        docs_by_id = {r["document_id"]: r for r in self.document_rows}
        pages_by_expense: Dict[tuple[str, str], Dict[str, str]] = {}
        rows_by_expense: Dict[tuple[str, str], List[Dict[str, str]]] = defaultdict(list)

        for page in self.page_rows:
            pages_by_expense[(page.get("competencia", ""), page.get("expense_id", ""))] = page

        for row in self.materialized_rows:
            rows_by_expense[(row["competencia"], row["expense_id"])].append(row)

        for (competencia, expense_id), rows in rows_by_expense.items():
            page = pages_by_expense.get((competencia, expense_id), {})
            docs = []

            for row in sorted(rows, key=lambda r: r["document_id"]):
                discovered = docs_by_id.get(row["document_id"], {})
                docs.append(
                    {
                        "document_id": row["document_id"],
                        "expense_page_version_id": row.get("expense_page_version_id"),
                        "document_uri": row.get("document_uri"),
                        "document_uri_sha256": row.get("document_uri_sha256"),
                        "document_link_source": discovered.get("document_link_source"),
                        "document_type_hint": discovered.get("document_type_hint"),
                        "saved_path_local": row.get("saved_path"),
                        "file_sha256": row.get("file_sha256"),
                        "file_size_bytes": int(float(row.get("file_size_bytes") or 0)),
                        "content_type": row.get("content_type"),
                        "download_status": row.get("download_status"),
                        "already_existed": row.get("already_existed") == "True",
                        "was_downloaded_now": row.get("was_downloaded_now") == "True",
                        "is_duplicate_file": row.get("is_duplicate_file") == "True",
                        "conflict_detected": row.get("conflict_detected") == "True",
                    }
                )

            payload = {
                "schema_name": "expense_evidence_manifest",
                "schema_version": "1.0",
                "run_id": self.logger.run_id,
                "competencia": competencia,
                "expense_id": expense_id,
                "expense_uri": page.get("expense_uri"),
                "expense_uri_sha256": page.get("expense_uri_sha256"),
                "final_url": page.get("final_url"),
                "final_url_sha256": page.get("final_url_sha256"),
                "expense_page_version_id": page.get("expense_page_version_id"),
                "html_saved_path": page.get("html_saved_path"),
                "html_sha256": page.get("html_sha256"),
                "document_count": len(docs),
                "documents": docs,
                "generated_at_utc": self.utc_now(),
            }

            path = self.local_manifest_path(competencia, expense_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        return len(rows_by_expense)

    def validate_local_materialization(self) -> None:
        failures = []

        for row in self.materialized_rows:
            saved_path = row.get("saved_path") or ""
            expense_id = row["expense_id"]
            expected_fragment = f"despesas/expense_id={expense_id}/documentos"

            if expected_fragment not in saved_path.replace("\\", "/"):
                failures.append((row["document_id"], "non_local_path", saved_path))
                continue

            path = Path(saved_path)
            if not path.exists():
                failures.append((row["document_id"], "missing_file", saved_path))
                continue

            actual_hash = self.sha256_file(path)
            actual_size = path.stat().st_size

            if row.get("file_sha256") != actual_hash:
                failures.append((row["document_id"], "hash_mismatch", saved_path))

            if int(float(row.get("file_size_bytes") or 0)) != actual_size:
                failures.append((row["document_id"], "size_mismatch", saved_path))

            if row.get("conflict_detected") == "True":
                failures.append((row["document_id"], "conflict", saved_path))

        if failures:
            sample = failures[:20]
            raise ValueError(f"Falha de materialização local: {sample}")

    def materialize(self) -> MaterializationResult:
        timer = ExecutionTimer()
        self.load_inputs()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="materialize",
            operation="expense_local_materialization",
            message="Materialização física local dos documentos de despesas iniciada.",
            records_in=len(self.download_rows),
        )

        self.materialized_rows = [self.materialize_row(row) for row in self.download_rows]
        manifest_count = self.write_local_manifests()
        self.validate_local_materialization()

        status = Counter(row.get("download_status") for row in self.materialized_rows)

        self.result = MaterializationResult(
            total_rows=len(self.materialized_rows),
            local_rows=len(self.materialized_rows),
            copied_rows=sum(1 for r in self.materialized_rows if r.get("was_downloaded_now") == "True" and r.get("already_existed") == "True"),
            downloaded_rows=sum(1 for r in self.materialized_rows if r.get("was_downloaded_now") == "True" and r.get("already_existed") != "True"),
            skipped_existing_rows=status.get("SKIPPED_EXISTING_DOCUMENT", 0),
            conflict_rows=sum(1 for r in self.materialized_rows if r.get("conflict_detected") == "True"),
            missing_rows=0,
            manifest_count=manifest_count,
        )

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="materialize",
            operation="expense_local_materialization",
            message="Materialização física local dos documentos de despesas concluída.",
            records_out=len(self.materialized_rows),
            duration_ms=timer.elapsed_ms(),
        )

        return self.result

    def write_outputs(self) -> Dict[str, Optional[str]]:
        if self.result is None:
            raise RuntimeError("Execute materialize() antes de write_outputs().")

        run_id = self.logger.run_id
        run_dir = self.output_dir / f"run_id={run_id}"
        csv_path = run_dir / "expense_document_downloads.csv"
        parquet_path = run_dir / "expense_document_downloads.parquet"
        summary_path = run_dir / "expense_local_materialization_summary.json"
        latest_path = self.output_dir / "expense_document_downloads_latest.csv"
        latest_summary_path = self.output_dir / "expense_local_materialization_summary_latest.json"

        fieldnames = list(self.download_rows[0].keys())
        for row in self.materialized_rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        self.write_csv(csv_path, self.materialized_rows, fieldnames)
        self.write_csv(latest_path, self.materialized_rows, fieldnames)
        parquet_written = self.write_parquet(parquet_path, self.materialized_rows)

        summary = {
            "approved": True,
            "status": "SUCCESS",
            "run_id": run_id,
            "total_rows": self.result.total_rows,
            "local_rows": self.result.local_rows,
            "copied_rows": self.result.copied_rows,
            "downloaded_rows": self.result.downloaded_rows,
            "skipped_existing_rows": self.result.skipped_existing_rows,
            "conflict_rows": self.result.conflict_rows,
            "missing_rows": self.result.missing_rows,
            "manifest_count": self.result.manifest_count,
        }

        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        latest_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "csv_path": str(csv_path),
            "latest_csv_path": str(latest_path),
            "parquet_path": str(parquet_written) if parquet_written else None,
            "summary_path": str(summary_path),
            "latest_summary_path": str(latest_summary_path),
        }

    def run(self) -> Dict[str, Optional[str]]:
        self.materialize()
        outputs = self.write_outputs()
        self.logger.close(require_parquet=self.require_parquet)
        return outputs
