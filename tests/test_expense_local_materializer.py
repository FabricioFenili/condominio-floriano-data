from __future__ import annotations

import csv
from pathlib import Path

from floriano.raw.expense_local_materializer import ExpenseLocalMaterializer
from floriano.utils.execution_logger import PipelineExecutionLogger


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_expense_local_materializer_copies_canonical_file_to_expense_folder(tmp_path: Path) -> None:
    canonical = tmp_path / "landing" / "downloads" / "2026_03" / "comprovantes" / "canonical.pdf"
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(b"same-content")

    import hashlib
    file_sha = hashlib.sha256(b"same-content").hexdigest()

    downloads_path = tmp_path / "expense_document_downloads_latest.csv"
    docs_path = tmp_path / "expense_documents_discovered_latest.csv"
    pages_path = tmp_path / "expense_pages_latest.csv"

    download_fields = [
        "run_id", "upstream_expense_documents_run_id", "expense_id", "expense_page_version_id",
        "document_id", "competencia", "document_uri", "document_uri_sha256",
        "duplicate_document_count", "download_attempted", "download_status", "http_status",
        "content_type", "content_disposition", "saved_path", "file_size_bytes", "file_sha256",
        "already_existed", "was_downloaded_now", "is_duplicate_document_uri", "is_duplicate_file",
        "conflict_detected", "error_type", "error_message", "downloaded_at_utc",
    ]
    doc_fields = [
        "run_id", "expense_id", "expense_page_version_id", "competencia", "document_id",
        "document_uri", "document_uri_sha256", "document_link_text", "document_link_source",
        "document_link_index", "document_type_hint", "discovered_at_utc",
    ]
    page_fields = [
        "run_id", "competencia", "expense_id", "expense_uri", "expense_uri_sha256",
        "final_url", "final_url_sha256", "html_saved_path", "html_sha256", "expense_page_version_id",
    ]

    write_csv(downloads_path, download_fields, [{
        "run_id": "run-downloads",
        "upstream_expense_documents_run_id": "run-docs",
        "expense_id": "expense-1",
        "expense_page_version_id": "page-1",
        "document_id": "document-1",
        "competencia": "2026_03",
        "document_uri": "https://example.com/doc.pdf",
        "document_uri_sha256": "uri-sha",
        "duplicate_document_count": "1",
        "download_attempted": "True",
        "download_status": "DUPLICATE_FILE_SHA",
        "http_status": "200",
        "content_type": "application/pdf",
        "content_disposition": "",
        "saved_path": str(canonical),
        "file_size_bytes": str(len(b"same-content")),
        "file_sha256": file_sha,
        "already_existed": "True",
        "was_downloaded_now": "True",
        "is_duplicate_document_uri": "False",
        "is_duplicate_file": "True",
        "conflict_detected": "False",
        "error_type": "",
        "error_message": "",
        "downloaded_at_utc": "2026-04-26T00:00:00+00:00",
    }])

    write_csv(docs_path, doc_fields, [{
        "run_id": "run-docs",
        "expense_id": "expense-1",
        "expense_page_version_id": "page-1",
        "competencia": "2026_03",
        "document_id": "document-1",
        "document_uri": "https://example.com/doc.pdf",
        "document_uri_sha256": "uri-sha",
        "document_link_text": "PDF",
        "document_link_source": "html_anchor",
        "document_link_index": "1",
        "document_type_hint": "document_link",
        "discovered_at_utc": "2026-04-26T00:00:00+00:00",
    }])

    write_csv(pages_path, page_fields, [{
        "run_id": "run-pages",
        "competencia": "2026_03",
        "expense_id": "expense-1",
        "expense_uri": "https://example.com/despesa",
        "expense_uri_sha256": "expense-uri-sha",
        "final_url": "https://example.com/despesa",
        "final_url_sha256": "final-url-sha",
        "html_saved_path": "",
        "html_sha256": "",
        "expense_page_version_id": "page-1",
    }])

    materializer = ExpenseLocalMaterializer(
        expense_downloads_path=downloads_path,
        expense_documents_path=docs_path,
        expense_pages_path=pages_path,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "expense_document_downloads",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-materializer"),
    )

    outputs = materializer.run()
    rows = read_csv(Path(outputs["latest_csv_path"]))

    assert len(rows) == 1
    assert rows[0]["saved_path"] != str(canonical)
    assert "/despesas/expense_id=expense-1/documentos/" in rows[0]["saved_path"]
    assert Path(rows[0]["saved_path"]).exists()
    assert Path(rows[0]["saved_path"]).read_bytes() == b"same-content"

    manifest_path = (
        tmp_path / "landing" / "downloads" / "2026_03" / "despesas" /
        "expense_id=expense-1" / "manifest" / "expense_evidence_manifest.json"
    )
    assert manifest_path.exists()
    assert "document-1" in manifest_path.read_text(encoding="utf-8")
