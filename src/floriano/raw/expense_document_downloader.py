from __future__ import annotations

import csv
import hashlib
import mimetypes
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import unquote, urlparse

from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class ExpenseDocumentDownloadRecord:
    run_id: str
    upstream_expense_documents_run_id: str
    expense_id: str
    expense_page_version_id: Optional[str]
    document_id: str
    competencia: str
    document_uri: str
    document_uri_sha256: str
    duplicate_document_count: int
    download_attempted: bool
    download_status: str
    http_status: Optional[int]
    content_type: Optional[str]
    content_disposition: Optional[str]
    saved_path: Optional[str]
    file_size_bytes: int
    file_sha256: Optional[str]
    already_existed: bool
    was_downloaded_now: bool
    is_duplicate_document_uri: bool
    is_duplicate_file: bool
    conflict_detected: bool
    error_type: Optional[str]
    error_message: Optional[str]
    downloaded_at_utc: str


class ExpenseDocumentDownloader:
    MODULE = "floriano.raw.expense_document_downloader"
    CLASS_NAME = "ExpenseDocumentDownloader"

    REQUIRED_COLUMNS = {
        "run_id",
        "expense_id",
        "expense_page_version_id",
        "competencia",
        "document_id",
        "document_uri",
        "document_uri_sha256",
    }

    EXPECTED_CONTENT_PREFIXES = (
        "application/pdf",
        "image/",
        "application/octet-stream",
    )

    def __init__(
        self,
        expense_documents_path: str | Path = "data/raw/expense_pages/expense_documents_discovered_latest.csv",
        landing_downloads_dir: str | Path = "data/landing/downloads",
        output_dir: str | Path = "data/raw/expense_document_downloads",
        logger: Optional[PipelineExecutionLogger] = None,
        session: Optional[object] = None,
        timeout_seconds: int = 60,
        skip_existing: bool = True,
        overwrite: bool = False,
        dry_run: bool = False,
        limit: Optional[int] = None,
        require_parquet: bool = False,
    ) -> None:
        if overwrite and skip_existing:
            raise ValueError("Configuração inválida: overwrite=True e skip_existing=True.")

        self.expense_documents_path = Path(expense_documents_path)
        self.landing_downloads_dir = Path(landing_downloads_dir)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.timeout_seconds = timeout_seconds
        self.skip_existing = skip_existing
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.limit = limit
        self.require_parquet = require_parquet

        if session is None:
            import requests
            self.session = requests.Session()
        else:
            self.session = session

        self.document_rows: List[Dict[str, str]] = []
        self.records: List[ExpenseDocumentDownloadRecord] = []

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def sha256_bytes(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @staticmethod
    def normalize_content_type(content_type: Optional[str]) -> str:
        if not content_type:
            return ""
        return content_type.lower().split(";")[0].strip()

    @classmethod
    def is_expected_content_type(cls, content_type: Optional[str]) -> bool:
        clean = cls.normalize_content_type(content_type)
        return any(clean.startswith(prefix) for prefix in cls.EXPECTED_CONTENT_PREFIXES)

    @staticmethod
    def extension_from_content_type(content_type: Optional[str]) -> str:
        clean = (content_type or "").lower().split(";")[0].strip()

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
    def filename_from_content_disposition(content_disposition: Optional[str]) -> Optional[str]:
        if not content_disposition:
            return None

        parts = [p.strip() for p in content_disposition.split(";")]

        for part in parts:
            if part.lower().startswith("filename*="):
                value = part.split("=", 1)[1].strip().strip('"')
                if "''" in value:
                    value = value.split("''", 1)[1]
                return unquote(value)

            if part.lower().startswith("filename="):
                return part.split("=", 1)[1].strip().strip('"')

        return None

    @staticmethod
    def filename_from_uri(uri: str) -> Optional[str]:
        parsed = urlparse(uri)
        name = Path(parsed.path).name
        return unquote(name) if name else None

    def load_documents(self) -> List[Dict[str, str]]:
        timer = ExecutionTimer()

        if not self.expense_documents_path.exists():
            raise FileNotFoundError(f"expense_documents_discovered não encontrado: {self.expense_documents_path}")

        with self.expense_documents_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = set(reader.fieldnames or [])

        missing = self.REQUIRED_COLUMNS - fieldnames
        if missing:
            raise ValueError(f"expense_documents_discovered sem colunas obrigatórias: {sorted(missing)}")

        self.document_rows = rows

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_documents",
            operation="load_expense_documents",
            message="Documentos de despesas carregados.",
            records_out=len(rows),
            duration_ms=timer.elapsed_ms(),
        )

        return rows

    @staticmethod
    def group_by_document_id(rows: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
        grouped: Dict[str, List[Dict[str, str]]] = {}

        for row in rows:
            grouped.setdefault(row["document_id"], []).append(row)

        return [
            {
                "document_id": key,
                "first_row": values[0],
                "duplicate_document_count": len(values),
                "is_duplicate_document_uri": len(values) > 1,
            }
            for key, values in grouped.items()
        ]

    def existing_file_for_document(self, competencia: str, expense_id: str, document_id: str) -> Optional[Path]:
        folder = self.landing_downloads_dir / competencia / "despesas" / f"expense_id={expense_id}" / "documentos"

        if not folder.exists():
            return None

        matches = sorted(folder.glob(f"{document_id}.*"))
        return matches[0] if matches else None

    def target_path(self, competencia: str, expense_id: str, document_id: str, content_type: Optional[str]) -> Path:
        folder = self.landing_downloads_dir / competencia / "despesas" / f"expense_id={expense_id}" / "documentos"
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{document_id}{self.extension_from_content_type(content_type)}"

    def known_file_hashes(self) -> Dict[str, str]:
        known: Dict[str, str] = {}
        base = self.landing_downloads_dir

        if not base.exists():
            return known

        for path in base.rglob("*"):
            if path.is_file() and path.name != ".gitkeep":
                try:
                    known[self.sha256_bytes(path.read_bytes())] = str(path)
                except Exception:
                    continue

        return known

    def _record(
        self,
        *,
        row: Dict[str, str],
        duplicate_document_count: int,
        download_attempted: bool,
        download_status: str,
        http_status: Optional[int],
        content_type: Optional[str],
        content_disposition: Optional[str],
        saved_path: Optional[str],
        file_size_bytes: int,
        file_sha256: Optional[str],
        already_existed: bool,
        was_downloaded_now: bool,
        is_duplicate_document_uri: bool,
        is_duplicate_file: bool,
        conflict_detected: bool,
        error_type: Optional[str],
        error_message: Optional[str],
    ) -> ExpenseDocumentDownloadRecord:
        return ExpenseDocumentDownloadRecord(
            run_id=self.logger.run_id,
            upstream_expense_documents_run_id=row.get("run_id", ""),
            expense_id=row.get("expense_id", ""),
            expense_page_version_id=row.get("expense_page_version_id") or None,
            document_id=row.get("document_id", ""),
            competencia=row.get("competencia", ""),
            document_uri=row.get("document_uri", ""),
            document_uri_sha256=row.get("document_uri_sha256", ""),
            duplicate_document_count=duplicate_document_count,
            download_attempted=download_attempted,
            download_status=download_status,
            http_status=http_status,
            content_type=content_type,
            content_disposition=content_disposition,
            saved_path=saved_path,
            file_size_bytes=file_size_bytes,
            file_sha256=file_sha256,
            already_existed=already_existed,
            was_downloaded_now=was_downloaded_now,
            is_duplicate_document_uri=is_duplicate_document_uri,
            is_duplicate_file=is_duplicate_file,
            conflict_detected=conflict_detected,
            error_type=error_type,
            error_message=error_message,
            downloaded_at_utc=self._utc_now(),
        )

    def process_group(self, group: Dict[str, object], known_hashes: Dict[str, str]) -> ExpenseDocumentDownloadRecord:
        row = group["first_row"]
        assert isinstance(row, dict)

        competencia = row["competencia"]
        expense_id = row["expense_id"]
        document_id = row["document_id"]
        document_uri = row["document_uri"]
        duplicate_count = int(group["duplicate_document_count"])
        is_dup_uri = bool(group["is_duplicate_document_uri"])

        existing = self.existing_file_for_document(competencia, expense_id, document_id)

        if existing and self.skip_existing and not self.overwrite:
            content = existing.read_bytes()
            file_sha = self.sha256_bytes(content)
            return self._record(
                row=row,
                duplicate_document_count=duplicate_count,
                download_attempted=False,
                download_status="SKIPPED_EXISTING_DOCUMENT",
                http_status=None,
                content_type=None,
                content_disposition=None,
                saved_path=str(existing),
                file_size_bytes=len(content),
                file_sha256=file_sha,
                already_existed=True,
                was_downloaded_now=False,
                is_duplicate_document_uri=is_dup_uri,
                is_duplicate_file=file_sha in known_hashes,
                conflict_detected=False,
                error_type=None,
                error_message=None,
            )

        if self.dry_run:
            return self._record(
                row=row,
                duplicate_document_count=duplicate_count,
                download_attempted=False,
                download_status="DRY_RUN",
                http_status=None,
                content_type=None,
                content_disposition=None,
                saved_path=None,
                file_size_bytes=0,
                file_sha256=None,
                already_existed=False,
                was_downloaded_now=False,
                is_duplicate_document_uri=is_dup_uri,
                is_duplicate_file=False,
                conflict_detected=False,
                error_type=None,
                error_message=None,
            )

        try:
            response = self.session.get(document_uri, timeout=self.timeout_seconds)
            http_status = int(getattr(response, "status_code", 0) or 0)
            headers = getattr(response, "headers", {}) or {}
            content_type = headers.get("content-type") or headers.get("Content-Type")
            content_disposition = headers.get("content-disposition") or headers.get("Content-Disposition")

            if http_status < 200 or http_status >= 300:
                return self._record(
                    row=row,
                    duplicate_document_count=duplicate_count,
                    download_attempted=True,
                    download_status="HTTP_ERROR",
                    http_status=http_status,
                    content_type=content_type,
                    content_disposition=content_disposition,
                    saved_path=None,
                    file_size_bytes=0,
                    file_sha256=None,
                    already_existed=False,
                    was_downloaded_now=False,
                    is_duplicate_document_uri=is_dup_uri,
                    is_duplicate_file=False,
                    conflict_detected=False,
                    error_type="HttpError",
                    error_message=f"HTTP status {http_status}",
                )

            content = bytes(getattr(response, "content", b"") or b"")

            if not content:
                return self._record(
                    row=row,
                    duplicate_document_count=duplicate_count,
                    download_attempted=True,
                    download_status="EMPTY_CONTENT",
                    http_status=http_status,
                    content_type=content_type,
                    content_disposition=content_disposition,
                    saved_path=None,
                    file_size_bytes=0,
                    file_sha256=None,
                    already_existed=False,
                    was_downloaded_now=False,
                    is_duplicate_document_uri=is_dup_uri,
                    is_duplicate_file=False,
                    conflict_detected=False,
                    error_type="EmptyContent",
                    error_message="Resposta HTTP sem conteúdo.",
                )

            if not self.is_expected_content_type(content_type):
                return self._record(
                    row=row,
                    duplicate_document_count=duplicate_count,
                    download_attempted=True,
                    download_status="UNEXPECTED_CONTENT_TYPE",
                    http_status=http_status,
                    content_type=content_type,
                    content_disposition=content_disposition,
                    saved_path=None,
                    file_size_bytes=len(content),
                    file_sha256=self.sha256_bytes(content),
                    already_existed=False,
                    was_downloaded_now=False,
                    is_duplicate_document_uri=is_dup_uri,
                    is_duplicate_file=False,
                    conflict_detected=False,
                    error_type="UnexpectedContentType",
                    error_message=f"Content-Type inesperado: {content_type}",
                )

            file_sha = self.sha256_bytes(content)

            if file_sha in known_hashes:
                return self._record(
                    row=row,
                    duplicate_document_count=duplicate_count,
                    download_attempted=True,
                    download_status="DUPLICATE_FILE_SHA",
                    http_status=http_status,
                    content_type=content_type,
                    content_disposition=content_disposition,
                    saved_path=known_hashes[file_sha],
                    file_size_bytes=len(content),
                    file_sha256=file_sha,
                    already_existed=True,
                    was_downloaded_now=True,
                    is_duplicate_document_uri=is_dup_uri,
                    is_duplicate_file=True,
                    conflict_detected=False,
                    error_type=None,
                    error_message=None,
                )

            target = self.target_path(competencia, expense_id, document_id, content_type)

            if target.exists() and not self.overwrite:
                current = target.read_bytes()
                current_sha = self.sha256_bytes(current)
                conflict = current_sha != file_sha

                return self._record(
                    row=row,
                    duplicate_document_count=duplicate_count,
                    download_attempted=True,
                    download_status="TARGET_EXISTS_CONFLICT" if conflict else "TARGET_EXISTS_SAME_HASH",
                    http_status=http_status,
                    content_type=content_type,
                    content_disposition=content_disposition,
                    saved_path=str(target),
                    file_size_bytes=len(current),
                    file_sha256=current_sha,
                    already_existed=True,
                    was_downloaded_now=False,
                    is_duplicate_document_uri=is_dup_uri,
                    is_duplicate_file=current_sha in known_hashes,
                    conflict_detected=conflict,
                    error_type="OverwriteBlocked" if conflict else None,
                    error_message="Arquivo alvo já existe e overwrite=False." if conflict else None,
                )

            target.write_bytes(content)
            known_hashes[file_sha] = str(target)

            return self._record(
                row=row,
                duplicate_document_count=duplicate_count,
                download_attempted=True,
                download_status="SUCCESS",
                http_status=http_status,
                content_type=content_type,
                content_disposition=content_disposition,
                saved_path=str(target),
                file_size_bytes=len(content),
                file_sha256=file_sha,
                already_existed=False,
                was_downloaded_now=True,
                is_duplicate_document_uri=is_dup_uri,
                is_duplicate_file=False,
                conflict_detected=False,
                error_type=None,
                error_message=None,
            )

        except Exception as exc:
            return self._record(
                row=row,
                duplicate_document_count=duplicate_count,
                download_attempted=True,
                download_status="REQUEST_EXCEPTION",
                http_status=None,
                content_type=None,
                content_disposition=None,
                saved_path=None,
                file_size_bytes=0,
                file_sha256=None,
                already_existed=False,
                was_downloaded_now=False,
                is_duplicate_document_uri=is_dup_uri,
                is_duplicate_file=False,
                conflict_detected=False,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )

    def download_documents(self) -> List[ExpenseDocumentDownloadRecord]:
        timer = ExecutionTimer()

        if not self.document_rows:
            self.load_documents()

        rows = self.document_rows

        if self.limit is not None:
            rows = rows[: self.limit]

        groups = self.group_by_document_id(rows)
        known = self.known_file_hashes()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="download_documents",
            operation="download_expense_documents",
            message="Iniciando download de documentos derivados de despesas.",
            records_in=len(rows),
        )

        self.records = [self.process_group(group, known) for group in groups]

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="download_documents",
            operation="download_expense_documents",
            message="Download de documentos derivados de despesas concluído.",
            records_in=len(rows),
            records_out=len(self.records),
            duration_ms=timer.elapsed_ms(),
        )

        return self.records

    def write_csv(self, rows: List[Dict[str, object]], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        fields = list(ExpenseDocumentDownloadRecord.__dataclass_fields__.keys())

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

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

    def write_output(self) -> Dict[str, Optional[str]]:
        rows = [asdict(r) for r in self.records]

        run_dir = self.output_dir / f"run_id={self.logger.run_id}"
        csv_path = run_dir / "expense_document_downloads.csv"
        parquet_path = run_dir / "expense_document_downloads.parquet"
        latest_path = self.output_dir / "expense_document_downloads_latest.csv"

        self.write_csv(rows, csv_path)
        self.write_csv(rows, latest_path)
        parquet_written = self.write_parquet(rows, parquet_path)

        return {
            "csv_path": str(csv_path),
            "latest_csv_path": str(latest_path),
            "parquet_path": str(parquet_written) if parquet_written else None,
        }

    def run(self) -> Dict[str, Optional[str]]:
        self.download_documents()
        outputs = self.write_output()
        self.logger.close(require_parquet=self.require_parquet)
        return outputs
