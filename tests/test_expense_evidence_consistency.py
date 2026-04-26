from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from floriano.raw.expense_evidence_consistency import ExpenseEvidenceConsistencyChecker
from floriano.utils.execution_logger import PipelineExecutionLogger


PAGE_FIELDS = [
    "run_id",
    "upstream_pdf_links_run_id",
    "competencia",
    "expense_id",
    "source_pdf_path",
    "source_pdf_sha256",
    "page_number",
    "expense_uri",
    "expense_uri_sha256",
    "final_url",
    "final_url_sha256",
    "http_status",
    "content_type",
    "redirect_count",
    "redirect_chain_json",
    "is_pdf_direct",
    "is_html_page",
    "html_saved_path",
    "html_sha256",
    "expense_page_version_id",
    "document_links_found",
    "crawl_status",
    "error_type",
    "error_message",
    "crawled_at_utc",
]

DOC_FIELDS = [
    "run_id",
    "expense_id",
    "expense_page_version_id",
    "competencia",
    "document_id",
    "document_uri",
    "document_uri_sha256",
    "document_link_text",
    "document_link_source",
    "document_link_index",
    "document_type_hint",
    "discovered_at_utc",
]

DOWNLOAD_FIELDS = [
    "run_id",
    "upstream_expense_documents_run_id",
    "expense_id",
    "expense_page_version_id",
    "document_id",
    "competencia",
    "document_uri",
    "document_uri_sha256",
    "duplicate_document_count",
    "download_attempted",
    "download_status",
    "http_status",
    "content_type",
    "content_disposition",
    "saved_path",
    "file_size_bytes",
    "file_sha256",
    "already_existed",
    "was_downloaded_now",
    "is_duplicate_document_uri",
    "is_duplicate_file",
    "conflict_detected",
    "error_type",
    "error_message",
    "downloaded_at_utc",
]


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def make_file(path: Path, content: bytes = b"%PDF fake") -> tuple[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return sha256_bytes(content), len(content)


def make_artifacts(tmp_path: Path) -> tuple[Path, Path, Path]:
    html_path = tmp_path / "landing" / "downloads" / "2026_03" / "despesas" / "expense_id=expense-1" / "page" / "page-version-1.html"
    html_sha, _ = make_file(html_path, b"<html><a href='/doc.pdf'>PDF</a></html>")

    doc_path = tmp_path / "landing" / "downloads" / "2026_03" / "despesas" / "expense_id=expense-1" / "documentos" / "document-1.pdf"
    file_sha, file_size = make_file(doc_path, b"%PDF fake")

    pages = tmp_path / "expense_pages.csv"
    docs = tmp_path / "expense_documents.csv"
    downloads = tmp_path / "expense_downloads.csv"

    write_csv(
        pages,
        PAGE_FIELDS,
        [
            {
                "run_id": "run-pages",
                "upstream_pdf_links_run_id": "run-links",
                "competencia": "2026_03",
                "expense_id": "expense-1",
                "source_pdf_path": "source.pdf",
                "source_pdf_sha256": "source-sha",
                "page_number": "1",
                "expense_uri": "https://example.com/despesa/1",
                "expense_uri_sha256": "expense-uri-sha",
                "final_url": "https://example.com/despesa/1",
                "final_url_sha256": "final-sha",
                "http_status": "200",
                "content_type": "text/html",
                "redirect_count": "0",
                "redirect_chain_json": "[]",
                "is_pdf_direct": "False",
                "is_html_page": "True",
                "html_saved_path": str(html_path),
                "html_sha256": html_sha,
                "expense_page_version_id": "page-version-1",
                "document_links_found": "1",
                "crawl_status": "HTML_PAGE_CRAWLED",
                "error_type": "",
                "error_message": "",
                "crawled_at_utc": "2026-04-26T00:00:00+00:00",
            }
        ],
    )

    write_csv(
        docs,
        DOC_FIELDS,
        [
            {
                "run_id": "run-docs",
                "expense_id": "expense-1",
                "expense_page_version_id": "page-version-1",
                "competencia": "2026_03",
                "document_id": "document-1",
                "document_uri": "https://example.com/doc.pdf",
                "document_uri_sha256": "doc-uri-sha",
                "document_link_text": "PDF",
                "document_link_source": "html_anchor",
                "document_link_index": "1",
                "document_type_hint": "document_link",
                "discovered_at_utc": "2026-04-26T00:00:00+00:00",
            }
        ],
    )

    write_csv(
        downloads,
        DOWNLOAD_FIELDS,
        [
            {
                "run_id": "run-downloads",
                "upstream_expense_documents_run_id": "run-docs",
                "expense_id": "expense-1",
                "expense_page_version_id": "page-version-1",
                "document_id": "document-1",
                "competencia": "2026_03",
                "document_uri": "https://example.com/doc.pdf",
                "document_uri_sha256": "doc-uri-sha",
                "duplicate_document_count": "1",
                "download_attempted": "True",
                "download_status": "SUCCESS",
                "http_status": "200",
                "content_type": "application/pdf",
                "content_disposition": "",
                "saved_path": str(doc_path),
                "file_size_bytes": str(file_size),
                "file_sha256": file_sha,
                "already_existed": "False",
                "was_downloaded_now": "True",
                "is_duplicate_document_uri": "False",
                "is_duplicate_file": "False",
                "conflict_detected": "False",
                "error_type": "",
                "error_message": "",
                "downloaded_at_utc": "2026-04-26T00:00:00+00:00",
            }
        ],
    )

    return pages, docs, downloads


def checker(tmp_path: Path, pages: Path, docs: Path, downloads: Path) -> ExpenseEvidenceConsistencyChecker:
    return ExpenseEvidenceConsistencyChecker(
        expense_pages_path=pages,
        expense_documents_path=docs,
        expense_downloads_path=downloads,
        output_dir=tmp_path / "quality",
        latest_registry_dir=tmp_path / "_latest",
        manifest_dir=tmp_path / "_manifests",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-check"),
        require_parquet=False,
    )


def test_expense_evidence_consistency_success(tmp_path: Path) -> None:
    pages, docs, downloads = make_artifacts(tmp_path)
    gate = checker(tmp_path, pages, docs, downloads)

    outputs = gate.run()

    assert Path(outputs["latest_csv_path"]).exists()
    assert Path(outputs["latest_summary_path"]).exists()
    assert Path(outputs["manifest_path"]).exists()
    assert '"approved": true' in Path(outputs["latest_summary_path"]).read_text(encoding="utf-8")


def test_expense_evidence_consistency_fails_on_missing_download(tmp_path: Path) -> None:
    pages, docs, downloads = make_artifacts(tmp_path)
    write_csv(downloads, DOWNLOAD_FIELDS, [])

    gate = checker(tmp_path, pages, docs, downloads)

    with pytest.raises(ValueError):
        gate.run()


def test_expense_evidence_consistency_fails_on_hash_mismatch(tmp_path: Path) -> None:
    pages, docs, downloads = make_artifacts(tmp_path)

    rows = []
    with downloads.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    rows[0]["file_sha256"] = "wrong-hash"
    write_csv(downloads, DOWNLOAD_FIELDS, rows)

    gate = checker(tmp_path, pages, docs, downloads)

    with pytest.raises(ValueError):
        gate.run()


def test_expense_evidence_consistency_accepts_direct_image_status(tmp_path: Path) -> None:
    pages, docs, downloads = make_artifacts(tmp_path)

    with pages.open(encoding="utf-8") as f:
        page_rows = list(csv.DictReader(f))

    page_rows[0]["crawl_status"] = "DIRECT_IMAGE_DISCOVERED"
    page_rows[0]["content_type"] = "image/jpeg"
    page_rows[0]["is_pdf_direct"] = "False"
    page_rows[0]["is_html_page"] = "False"
    page_rows[0]["html_saved_path"] = ""
    page_rows[0]["html_sha256"] = ""
    page_rows[0]["expense_page_version_id"] = ""

    write_csv(pages, PAGE_FIELDS, page_rows)

    gate = checker(tmp_path, pages, docs, downloads)
    outputs = gate.run()

    assert Path(outputs["latest_summary_path"]).exists()
    assert '"approved": true' in Path(outputs["latest_summary_path"]).read_text(encoding="utf-8")
