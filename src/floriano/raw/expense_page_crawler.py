from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class ExpensePageRecord:
    run_id: str
    upstream_pdf_links_run_id: str
    competencia: str
    expense_id: str
    source_pdf_path: str
    source_pdf_sha256: str
    page_number: int
    expense_uri: str
    expense_uri_sha256: str
    final_url: str
    final_url_sha256: str
    http_status: Optional[int]
    content_type: Optional[str]
    redirect_count: int
    redirect_chain_json: str
    is_pdf_direct: bool
    is_html_page: bool
    html_saved_path: Optional[str]
    html_sha256: Optional[str]
    expense_page_version_id: Optional[str]
    document_links_found: int
    crawl_status: str
    error_type: Optional[str]
    error_message: Optional[str]
    crawled_at_utc: str


@dataclass(frozen=True)
class ExpenseDocumentDiscoveredRecord:
    run_id: str
    expense_id: str
    expense_page_version_id: Optional[str]
    competencia: str
    document_id: str
    document_uri: str
    document_uri_sha256: str
    document_link_text: Optional[str]
    document_link_source: str
    document_link_index: int
    document_type_hint: str
    discovered_at_utc: str


class ExpensePageCrawler:
    MODULE = "floriano.raw.expense_page_crawler"
    CLASS_NAME = "ExpensePageCrawler"

    REQUIRED_COLUMNS = {
        "run_id",
        "competencia",
        "source_path",
        "source_sha256",
        "page_number",
        "uri",
        "uri_sha256",
        "link_type",
    }

    def __init__(
        self,
        pdf_links_path: str | Path = "data/raw/pdf_links/pdf_links_latest.csv",
        landing_downloads_dir: str | Path = "data/landing/downloads",
        output_dir: str | Path = "data/raw/expense_pages",
        logger: Optional[PipelineExecutionLogger] = None,
        session: Optional[object] = None,
        timeout_seconds: int = 60,
        base_url: Optional[str] = None,
        dry_run: bool = False,
        limit: Optional[int] = None,
        require_parquet: bool = False,
    ) -> None:
        self.pdf_links_path = Path(pdf_links_path)
        self.landing_downloads_dir = Path(landing_downloads_dir)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.timeout_seconds = timeout_seconds
        self.base_url = base_url
        self.dry_run = dry_run
        self.limit = limit
        self.require_parquet = require_parquet

        if session is None:
            import requests
            self.session = requests.Session()
        else:
            self.session = session

        self.pdf_link_rows: List[Dict[str, str]] = []
        self.page_records: List[ExpensePageRecord] = []
        self.document_records: List[ExpenseDocumentDiscoveredRecord] = []

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def sha256_text(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def sha256_bytes(value: bytes) -> str:
        return hashlib.sha256(value).hexdigest()

    @classmethod
    def make_expense_id(cls, competencia: str, expense_uri: str) -> str:
        return cls.sha256_text(f"{competencia}|{expense_uri}")

    @classmethod
    def make_document_id(cls, expense_id: str, document_uri: str) -> str:
        return cls.sha256_text(f"{expense_id}|{document_uri}")

    @classmethod
    def make_page_version_id(
        cls,
        *,
        expense_id: str,
        final_url: str,
        http_status: Optional[int],
        html_sha256: str,
    ) -> str:
        return cls.sha256_text(f"{expense_id}|{final_url}|{http_status}|{html_sha256}")

    @staticmethod
    def normalize_content_type(content_type: Optional[str]) -> str:
        if not content_type:
            return ""
        return content_type.lower().split(";")[0].strip()

    @classmethod
    def is_pdf_content(cls, content_type: Optional[str]) -> bool:
        return cls.normalize_content_type(content_type) == "application/pdf"

    @classmethod
    def is_image_content(cls, content_type: Optional[str]) -> bool:
        return cls.normalize_content_type(content_type).startswith("image/")

    @classmethod
    def is_binary_content(cls, content_type: Optional[str]) -> bool:
        return cls.normalize_content_type(content_type) == "application/octet-stream"

    @classmethod
    def is_direct_document_content(cls, content_type: Optional[str]) -> bool:
        return (
            cls.is_pdf_content(content_type)
            or cls.is_image_content(content_type)
            or cls.is_binary_content(content_type)
        )

    @classmethod
    def is_html_content(cls, content_type: Optional[str]) -> bool:
        return cls.normalize_content_type(content_type) in {"text/html", "application/xhtml+xml"}

    @staticmethod
    def is_obvious_non_evidence_asset(href: str) -> bool:
        value = (href or "").lower()

        blocked_tokens = (
            "favicon",
            "sprite",
            "logo",
            "loading",
            "spinner",
            "blank.gif",
            "/assets/",
            "/static/",
            "/css/",
            "/js/",
            "bootstrap",
            "jquery",
            "fontawesome",
            "glyphicons",
        )

        return any(token in value for token in blocked_tokens)

    @staticmethod
    def looks_like_document_href(href: str) -> bool:
        value = (href or "").lower().strip()

        if not value:
            return False

        if value.startswith("data:"):
            return False

        if ExpensePageCrawler.is_obvious_non_evidence_asset(value):
            return False

        document_tokens = (
            "downloadarquivo",
            "comprovante",
            "documento",
            ".pdf",
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
            ".gif",
            ".bin",
        )

        return any(token in value for token in document_tokens)

    def normalize_uri(self, uri: str) -> str:
        uri = (uri or "").strip()

        if uri.startswith("http://") or uri.startswith("https://"):
            return uri

        if uri.startswith("/") and self.base_url:
            return urljoin(self.base_url.rstrip("/") + "/", uri.lstrip("/"))

        return uri

    def load_pdf_links(self) -> List[Dict[str, str]]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_pdf_links",
            operation="load_pdf_links",
            message="Carregando pdf_links latest para fluxo de despesas.",
            source_file=str(self.pdf_links_path),
        )

        if not self.pdf_links_path.exists():
            raise FileNotFoundError(f"pdf_links não encontrado: {self.pdf_links_path}")

        with self.pdf_links_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = set(reader.fieldnames or [])

        missing = self.REQUIRED_COLUMNS - fieldnames
        if missing:
            raise ValueError(f"pdf_links sem colunas obrigatórias: {sorted(missing)}")

        self.pdf_link_rows = rows

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_pdf_links",
            operation="load_pdf_links",
            message="pdf_links carregado para fluxo de despesas.",
            records_out=len(rows),
            duration_ms=timer.elapsed_ms(),
        )

        return rows

    def filter_expense_links(self) -> List[Dict[str, str]]:
        if not self.pdf_link_rows:
            self.load_pdf_links()

        rows = [
            r for r in self.pdf_link_rows
            if r.get("link_type") == "despesa"
        ]

        if self.limit is not None:
            rows = rows[: self.limit]

        return rows

    @staticmethod
    def group_by_expense_uri(rows: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
        groups: Dict[str, List[Dict[str, str]]] = {}

        for row in rows:
            competencia = row.get("competencia", "")
            uri = row.get("uri", "")
            key = ExpensePageCrawler.make_expense_id(competencia, uri)
            groups.setdefault(key, []).append(row)

        return [
            {
                "expense_id": key,
                "first_row": values[0],
                "duplicate_expense_uri_count": len(values),
            }
            for key, values in groups.items()
        ]

    def expense_page_dir(self, competencia: str, expense_id: str) -> Path:
        return self.landing_downloads_dir / competencia / "despesas" / f"expense_id={expense_id}" / "page"

    def save_html_snapshot(
        self,
        *,
        competencia: str,
        expense_id: str,
        page_version_id: str,
        html_bytes: bytes,
        metadata: Dict[str, object],
    ) -> Dict[str, str]:
        page_dir = self.expense_page_dir(competencia, expense_id)
        page_dir.mkdir(parents=True, exist_ok=True)

        html_path = page_dir / f"{page_version_id}.html"
        meta_path = page_dir / f"{page_version_id}.json"

        if not html_path.exists():
            html_path.write_bytes(html_bytes)

        if not meta_path.exists():
            meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "html_saved_path": str(html_path),
            "metadata_path": str(meta_path),
        }

    def extract_document_links_from_html(
        self,
        *,
        html_bytes: bytes,
        base_url: str,
        expense_id: str,
        page_version_id: str,
        competencia: str,
    ) -> List[ExpenseDocumentDiscoveredRecord]:
        soup = BeautifulSoup(html_bytes, "html.parser")
        records: List[ExpenseDocumentDiscoveredRecord] = []
        seen: set[str] = set()

        def add_candidate(
            *,
            raw_uri: str,
            link_text: Optional[str],
            link_source: str,
            link_index: int,
            type_hint: str,
        ) -> None:
            if not raw_uri:
                return

            if not self.looks_like_document_href(raw_uri):
                return

            document_uri = urljoin(base_url, raw_uri)
            document_uri_sha = self.sha256_text(document_uri)

            if document_uri_sha in seen:
                return

            seen.add(document_uri_sha)

            document_id = self.make_document_id(expense_id, document_uri)

            records.append(
                ExpenseDocumentDiscoveredRecord(
                    run_id=self.logger.run_id,
                    expense_id=expense_id,
                    expense_page_version_id=page_version_id,
                    competencia=competencia,
                    document_id=document_id,
                    document_uri=document_uri,
                    document_uri_sha256=document_uri_sha,
                    document_link_text=link_text,
                    document_link_source=link_source,
                    document_link_index=link_index,
                    document_type_hint=type_hint,
                    discovered_at_utc=self._utc_now(),
                )
            )

        for idx, tag in enumerate(soup.find_all("a"), start=1):
            add_candidate(
                raw_uri=tag.get("href") or "",
                link_text=tag.get_text(" ", strip=True) or None,
                link_source="html_anchor",
                link_index=idx,
                type_hint="html_anchor_document",
            )

        for idx, tag in enumerate(soup.find_all("img"), start=1):
            add_candidate(
                raw_uri=tag.get("src") or "",
                link_text=tag.get("alt") or tag.get("title") or None,
                link_source="html_img_src",
                link_index=idx,
                type_hint="html_image",
            )

        for idx, tag in enumerate(soup.find_all("object"), start=1):
            add_candidate(
                raw_uri=tag.get("data") or "",
                link_text=tag.get("title") or None,
                link_source="html_object_data",
                link_index=idx,
                type_hint="html_object",
            )

        for idx, tag in enumerate(soup.find_all("embed"), start=1):
            add_candidate(
                raw_uri=tag.get("src") or "",
                link_text=tag.get("title") or None,
                link_source="html_embed_src",
                link_index=idx,
                type_hint="html_embed",
            )

        for idx, tag in enumerate(soup.find_all("iframe"), start=1):
            add_candidate(
                raw_uri=tag.get("src") or "",
                link_text=tag.get("title") or None,
                link_source="html_iframe_src",
                link_index=idx,
                type_hint="html_iframe",
            )

        return records

    def process_group(self, group: Dict[str, object]) -> None:
        row = group["first_row"]
        assert isinstance(row, dict)

        competencia = row.get("competencia", "")
        raw_uri = row.get("uri", "")
        expense_uri = self.normalize_uri(raw_uri)
        expense_uri_sha = self.sha256_text(expense_uri)
        expense_id = str(group["expense_id"])

        if self.dry_run:
            self.page_records.append(
                ExpensePageRecord(
                    run_id=self.logger.run_id,
                    upstream_pdf_links_run_id=row.get("run_id", ""),
                    competencia=competencia,
                    expense_id=expense_id,
                    source_pdf_path=row.get("source_path", ""),
                    source_pdf_sha256=row.get("source_sha256", ""),
                    page_number=int(float(row.get("page_number") or 0)),
                    expense_uri=expense_uri,
                    expense_uri_sha256=expense_uri_sha,
                    final_url="",
                    final_url_sha256="",
                    http_status=None,
                    content_type=None,
                    redirect_count=0,
                    redirect_chain_json="[]",
                    is_pdf_direct=False,
                    is_html_page=False,
                    html_saved_path=None,
                    html_sha256=None,
                    expense_page_version_id=None,
                    document_links_found=0,
                    crawl_status="DRY_RUN",
                    error_type=None,
                    error_message=None,
                    crawled_at_utc=self._utc_now(),
                )
            )
            return

        try:
            response = self.session.get(expense_uri, timeout=self.timeout_seconds)
            http_status = int(getattr(response, "status_code", 0) or 0)
            headers = getattr(response, "headers", {}) or {}
            content_type = headers.get("content-type") or headers.get("Content-Type")
            content = bytes(getattr(response, "content", b"") or b"")
            final_url = str(getattr(response, "url", expense_uri) or expense_uri)
            history = getattr(response, "history", []) or []
            redirect_chain = [str(getattr(item, "url", "")) for item in history if getattr(item, "url", "")]

            if http_status < 200 or http_status >= 300:
                self.page_records.append(
                    ExpensePageRecord(
                        run_id=self.logger.run_id,
                        upstream_pdf_links_run_id=row.get("run_id", ""),
                        competencia=competencia,
                        expense_id=expense_id,
                        source_pdf_path=row.get("source_path", ""),
                        source_pdf_sha256=row.get("source_sha256", ""),
                        page_number=int(float(row.get("page_number") or 0)),
                        expense_uri=expense_uri,
                        expense_uri_sha256=expense_uri_sha,
                        final_url=final_url,
                        final_url_sha256=self.sha256_text(final_url),
                        http_status=http_status,
                        content_type=content_type,
                        redirect_count=len(redirect_chain),
                        redirect_chain_json=json.dumps(redirect_chain, ensure_ascii=False),
                        is_pdf_direct=False,
                        is_html_page=False,
                        html_saved_path=None,
                        html_sha256=None,
                        expense_page_version_id=None,
                        document_links_found=0,
                        crawl_status="HTTP_ERROR",
                        error_type="HttpError",
                        error_message=f"HTTP status {http_status}",
                        crawled_at_utc=self._utc_now(),
                    )
                )
                return

            if self.is_direct_document_content(content_type):
                if self.is_pdf_content(content_type):
                    document_link_source = "direct_pdf_response"
                    document_type_hint = "direct_pdf"
                    crawl_status = "DIRECT_PDF_DISCOVERED"
                    is_pdf_direct = True
                elif self.is_image_content(content_type):
                    document_link_source = "direct_image_response"
                    document_type_hint = "direct_image"
                    crawl_status = "DIRECT_IMAGE_DISCOVERED"
                    is_pdf_direct = False
                else:
                    document_link_source = "direct_binary_response"
                    document_type_hint = "direct_binary"
                    crawl_status = "DIRECT_BINARY_DISCOVERED"
                    is_pdf_direct = False

                document_id = self.make_document_id(expense_id, final_url)

                self.document_records.append(
                    ExpenseDocumentDiscoveredRecord(
                        run_id=self.logger.run_id,
                        expense_id=expense_id,
                        expense_page_version_id=None,
                        competencia=competencia,
                        document_id=document_id,
                        document_uri=final_url,
                        document_uri_sha256=self.sha256_text(final_url),
                        document_link_text=None,
                        document_link_source=document_link_source,
                        document_link_index=1,
                        document_type_hint=document_type_hint,
                        discovered_at_utc=self._utc_now(),
                    )
                )

                self.page_records.append(
                    ExpensePageRecord(
                        run_id=self.logger.run_id,
                        upstream_pdf_links_run_id=row.get("run_id", ""),
                        competencia=competencia,
                        expense_id=expense_id,
                        source_pdf_path=row.get("source_path", ""),
                        source_pdf_sha256=row.get("source_sha256", ""),
                        page_number=int(float(row.get("page_number") or 0)),
                        expense_uri=expense_uri,
                        expense_uri_sha256=expense_uri_sha,
                        final_url=final_url,
                        final_url_sha256=self.sha256_text(final_url),
                        http_status=http_status,
                        content_type=content_type,
                        redirect_count=len(redirect_chain),
                        redirect_chain_json=json.dumps(redirect_chain, ensure_ascii=False),
                        is_pdf_direct=is_pdf_direct,
                        is_html_page=False,
                        html_saved_path=None,
                        html_sha256=None,
                        expense_page_version_id=None,
                        document_links_found=1,
                        crawl_status=crawl_status,
                        error_type=None,
                        error_message=None,
                        crawled_at_utc=self._utc_now(),
                    )
                )
                return

            if self.is_html_content(content_type):
                html_sha = self.sha256_bytes(content)
                page_version_id = self.make_page_version_id(
                    expense_id=expense_id,
                    final_url=final_url,
                    http_status=http_status,
                    html_sha256=html_sha,
                )

                metadata = {
                    "expense_id": expense_id,
                    "expense_uri": expense_uri,
                    "final_url": final_url,
                    "http_status": http_status,
                    "content_type": content_type,
                    "html_sha256": html_sha,
                    "captured_at_utc": self._utc_now(),
                }

                saved = self.save_html_snapshot(
                    competencia=competencia,
                    expense_id=expense_id,
                    page_version_id=page_version_id,
                    html_bytes=content,
                    metadata=metadata,
                )

                discovered = self.extract_document_links_from_html(
                    html_bytes=content,
                    base_url=final_url,
                    expense_id=expense_id,
                    page_version_id=page_version_id,
                    competencia=competencia,
                )

                self.document_records.extend(discovered)

                self.page_records.append(
                    ExpensePageRecord(
                        run_id=self.logger.run_id,
                        upstream_pdf_links_run_id=row.get("run_id", ""),
                        competencia=competencia,
                        expense_id=expense_id,
                        source_pdf_path=row.get("source_path", ""),
                        source_pdf_sha256=row.get("source_sha256", ""),
                        page_number=int(float(row.get("page_number") or 0)),
                        expense_uri=expense_uri,
                        expense_uri_sha256=expense_uri_sha,
                        final_url=final_url,
                        final_url_sha256=self.sha256_text(final_url),
                        http_status=http_status,
                        content_type=content_type,
                        redirect_count=len(redirect_chain),
                        redirect_chain_json=json.dumps(redirect_chain, ensure_ascii=False),
                        is_pdf_direct=False,
                        is_html_page=True,
                        html_saved_path=saved["html_saved_path"],
                        html_sha256=html_sha,
                        expense_page_version_id=page_version_id,
                        document_links_found=len(discovered),
                        crawl_status="HTML_PAGE_CRAWLED",
                        error_type=None if discovered else "NoDocumentLinkFound",
                        error_message=None if discovered else "Nenhum link documental encontrado na página.",
                        crawled_at_utc=self._utc_now(),
                    )
                )
                return

            self.page_records.append(
                ExpensePageRecord(
                    run_id=self.logger.run_id,
                    upstream_pdf_links_run_id=row.get("run_id", ""),
                    competencia=competencia,
                    expense_id=expense_id,
                    source_pdf_path=row.get("source_path", ""),
                    source_pdf_sha256=row.get("source_sha256", ""),
                    page_number=int(float(row.get("page_number") or 0)),
                    expense_uri=expense_uri,
                    expense_uri_sha256=expense_uri_sha,
                    final_url=final_url,
                    final_url_sha256=self.sha256_text(final_url),
                    http_status=http_status,
                    content_type=content_type,
                    redirect_count=len(redirect_chain),
                    redirect_chain_json=json.dumps(redirect_chain, ensure_ascii=False),
                    is_pdf_direct=False,
                    is_html_page=False,
                    html_saved_path=None,
                    html_sha256=None,
                    expense_page_version_id=None,
                    document_links_found=0,
                    crawl_status="UNSUPPORTED_CONTENT_TYPE",
                    error_type="UnsupportedContentType",
                    error_message=f"Content-Type não suportado: {content_type}",
                    crawled_at_utc=self._utc_now(),
                )
            )

        except Exception as exc:
            self.page_records.append(
                ExpensePageRecord(
                    run_id=self.logger.run_id,
                    upstream_pdf_links_run_id=row.get("run_id", ""),
                    competencia=competencia,
                    expense_id=expense_id,
                    source_pdf_path=row.get("source_path", ""),
                    source_pdf_sha256=row.get("source_sha256", ""),
                    page_number=int(float(row.get("page_number") or 0)),
                    expense_uri=expense_uri,
                    expense_uri_sha256=expense_uri_sha,
                    final_url="",
                    final_url_sha256="",
                    http_status=None,
                    content_type=None,
                    redirect_count=0,
                    redirect_chain_json="[]",
                    is_pdf_direct=False,
                    is_html_page=False,
                    html_saved_path=None,
                    html_sha256=None,
                    expense_page_version_id=None,
                    document_links_found=0,
                    crawl_status="REQUEST_EXCEPTION",
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                    crawled_at_utc=self._utc_now(),
                )
            )

    def crawl(self) -> Dict[str, List[object]]:
        timer = ExecutionTimer()
        rows = self.filter_expense_links()
        groups = self.group_by_expense_uri(rows)

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="crawl",
            operation="crawl_expense_pages",
            message="Iniciando crawl de páginas de despesas.",
            records_in=len(rows),
        )

        for group in groups:
            self.process_group(group)

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="crawl",
            operation="crawl_expense_pages",
            message="Crawl de páginas de despesas concluído.",
            records_in=len(rows),
            records_out=len(self.page_records),
            duration_ms=timer.elapsed_ms(),
        )

        return {
            "pages": self.page_records,
            "documents": self.document_records,
        }

    @staticmethod
    def write_csv(rows: List[Dict[str, object]], path: Path, fieldnames: List[str]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
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
        timer = ExecutionTimer()

        page_rows = [asdict(r) for r in self.page_records]
        doc_rows = [asdict(r) for r in self.document_records]

        run_dir = self.output_dir / f"run_id={self.logger.run_id}"

        pages_csv = run_dir / "expense_pages.csv"
        docs_csv = run_dir / "expense_documents_discovered.csv"
        pages_parquet = run_dir / "expense_pages.parquet"
        docs_parquet = run_dir / "expense_documents_discovered.parquet"

        pages_latest = self.output_dir / "expense_pages_latest.csv"
        docs_latest = self.output_dir / "expense_documents_discovered_latest.csv"

        page_fields = list(ExpensePageRecord.__dataclass_fields__.keys())
        doc_fields = list(ExpenseDocumentDiscoveredRecord.__dataclass_fields__.keys())

        self.write_csv(page_rows, pages_csv, page_fields)
        self.write_csv(doc_rows, docs_csv, doc_fields)
        self.write_csv(page_rows, pages_latest, page_fields)
        self.write_csv(doc_rows, docs_latest, doc_fields)

        pages_parquet_written = self.write_parquet(page_rows, pages_parquet)
        docs_parquet_written = self.write_parquet(doc_rows, docs_parquet)

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="write_output",
            operation="write_expense_page_outputs",
            message="Artefatos RAW de despesas gravados.",
            records_out=len(page_rows) + len(doc_rows),
            duration_ms=timer.elapsed_ms(),
            source_file=str(pages_csv),
        )

        return {
            "expense_pages_csv_path": str(pages_csv),
            "expense_pages_latest_path": str(pages_latest),
            "expense_pages_parquet_path": str(pages_parquet_written) if pages_parquet_written else None,
            "expense_documents_csv_path": str(docs_csv),
            "expense_documents_latest_path": str(docs_latest),
            "expense_documents_parquet_path": str(docs_parquet_written) if docs_parquet_written else None,
        }

    def run(self) -> Dict[str, Optional[str]]:
        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="run",
            operation="expense_page_crawler_pipeline",
            message="Pipeline de crawl de despesas iniciado.",
        )

        try:
            self.crawl()
            outputs = self.write_output()
            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="expense_page_crawler_pipeline",
                message="Pipeline de crawl de despesas finalizado.",
                records_out=len(self.page_records) + len(self.document_records),
            )
            self.logger.close(require_parquet=self.require_parquet)
            return outputs
        except Exception as exc:
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="expense_page_crawler_pipeline",
                message="Pipeline de crawl de despesas falhou.",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )
            self.logger.close(require_parquet=False)
            raise
