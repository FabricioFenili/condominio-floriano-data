from __future__ import annotations

import csv
from pathlib import Path

from floriano.raw.expense_document_downloader import ExpenseDocumentDownloader
from floriano.utils.execution_logger import PipelineExecutionLogger


class FakeResponse:
    def __init__(self, status_code: int, content: bytes, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}


class FakeSession:
    def __init__(self, responses: dict[str, object]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, timeout: int = 60) -> object:
        self.calls.append(url)
        result = self.responses[url]

        if isinstance(result, Exception):
            raise result

        return result


def write_docs(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
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

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def doc_row(uri: str = "https://example.com/doc.pdf") -> dict[str, str]:
    return {
        "run_id": "run-docs",
        "expense_id": "expense-1",
        "expense_page_version_id": "page-version-1",
        "competencia": "2026_03",
        "document_id": "document-1",
        "document_uri": uri,
        "document_uri_sha256": "uri-sha-1",
        "document_link_text": "PDF",
        "document_link_source": "html_anchor",
        "document_link_index": "1",
        "document_type_hint": "document_link",
        "discovered_at_utc": "2026-04-26T00:00:00+00:00",
    }


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_expense_document_downloader_success(tmp_path: Path) -> None:
    uri = "https://example.com/doc.pdf"
    docs_path = tmp_path / "docs.csv"
    write_docs(docs_path, [doc_row(uri)])

    session = FakeSession({uri: FakeResponse(200, b"%PDF fake", {"content-type": "application/pdf"})})

    downloader = ExpenseDocumentDownloader(
        expense_documents_path=docs_path,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "expense_document_downloads",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = downloader.run()
    rows = read_csv(Path(outputs["latest_csv_path"]))

    assert len(rows) == 1
    assert rows[0]["download_status"] == "SUCCESS"
    assert rows[0]["was_downloaded_now"] == "True"
    assert Path(rows[0]["saved_path"]).exists()
    assert "/despesas/expense_id=expense-1/documentos/" in rows[0]["saved_path"]


def test_expense_document_downloader_skip_existing(tmp_path: Path) -> None:
    uri = "https://example.com/doc.pdf"
    docs_path = tmp_path / "docs.csv"
    write_docs(docs_path, [doc_row(uri)])

    existing = tmp_path / "landing" / "downloads" / "2026_03" / "despesas" / "expense_id=expense-1" / "documentos" / "document-1.pdf"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(b"existing")

    session = FakeSession({uri: FakeResponse(200, b"%PDF new", {"content-type": "application/pdf"})})

    downloader = ExpenseDocumentDownloader(
        expense_documents_path=docs_path,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "expense_document_downloads",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
        skip_existing=True,
        overwrite=False,
    )

    outputs = downloader.run()
    rows = read_csv(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "SKIPPED_EXISTING_DOCUMENT"
    assert rows[0]["already_existed"] == "True"
    assert len(session.calls) == 0
    assert existing.read_bytes() == b"existing"


def test_expense_document_downloader_http_error(tmp_path: Path) -> None:
    uri = "https://example.com/doc.pdf"
    docs_path = tmp_path / "docs.csv"
    write_docs(docs_path, [doc_row(uri)])

    session = FakeSession({uri: FakeResponse(404, b"no", {"content-type": "text/plain"})})

    downloader = ExpenseDocumentDownloader(
        expense_documents_path=docs_path,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "expense_document_downloads",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = downloader.run()
    rows = read_csv(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "HTTP_ERROR"
    assert rows[0]["http_status"] == "404"


def test_expense_document_downloader_saves_image_with_correct_extension(tmp_path: Path) -> None:
    uri = "https://example.com/doc.jpg"
    docs_path = tmp_path / "docs.csv"
    write_docs(docs_path, [doc_row(uri)])

    session = FakeSession({uri: FakeResponse(200, b"fake-jpeg", {"content-type": "image/jpeg"})})

    downloader = ExpenseDocumentDownloader(
        expense_documents_path=docs_path,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "expense_document_downloads",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = downloader.run()
    rows = read_csv(Path(outputs["latest_csv_path"]))

    assert len(rows) == 1
    assert rows[0]["download_status"] == "SUCCESS"
    assert rows[0]["content_type"] == "image/jpeg"
    assert rows[0]["saved_path"].endswith(".jpg")
    assert Path(rows[0]["saved_path"]).exists()
