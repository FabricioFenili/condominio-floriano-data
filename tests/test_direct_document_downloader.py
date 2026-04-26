from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from floriano.raw.direct_document_downloader import DirectDocumentDownloader
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


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def write_pdf_links(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "run_id",
        "competencia",
        "source_path",
        "source_sha256",
        "page_number",
        "uri",
        "uri_sha256",
        "link_type",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def link_row(uri: str, competencia: str = "2026_03", link_type: str = "downloadarquivo") -> dict[str, str]:
    return {
        "run_id": "run-links",
        "competencia": competencia,
        "source_path": "source.pdf",
        "source_sha256": "source-sha",
        "page_number": "1",
        "uri": uri,
        "uri_sha256": sha256_text(uri),
        "link_type": link_type,
    }


def read_latest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_direct_document_downloader_success(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"
    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri)])

    session = FakeSession(
        {
            uri: FakeResponse(
                200,
                b"%PDF-1.7 fake",
                {"content-type": "application/pdf", "content-disposition": 'attachment; filename="a.pdf"'},
            )
        }
    )

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert len(rows) == 1
    assert rows[0]["download_status"] == "SUCCESS"
    assert rows[0]["was_downloaded_now"] == "True"
    assert rows[0]["saved_path"].endswith(".pdf")
    assert Path(rows[0]["saved_path"]).exists()


def test_direct_document_downloader_filters_only_downloadarquivo(tmp_path: Path) -> None:
    uri_download = "https://example.com/downloadarquivo?id=1"
    uri_despesa = "https://example.com/despesas/1"

    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(
        pdf_links,
        [
            link_row(uri_download, link_type="downloadarquivo"),
            link_row(uri_despesa, link_type="despesa"),
        ],
    )

    session = FakeSession(
        {
            uri_download: FakeResponse(200, b"%PDF", {"content-type": "application/pdf"}),
        }
    )

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert len(rows) == 1
    assert rows[0]["uri"] == uri_download


def test_direct_document_downloader_deduplicates_uri_before_download(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"

    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri), link_row(uri)])

    session = FakeSession(
        {
            uri: FakeResponse(200, b"%PDF", {"content-type": "application/pdf"}),
        }
    )

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert len(rows) == 1
    assert len(session.calls) == 1
    assert rows[0]["is_duplicate_uri"] == "True"
    assert rows[0]["duplicate_uri_count"] == "2"


def test_direct_document_downloader_skip_existing(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"
    uri_sha = sha256_text(uri)

    existing = tmp_path / "landing" / "downloads" / "2026_03" / "comprovantes" / f"{uri_sha}.pdf"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(b"existing")

    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri)])

    session = FakeSession(
        {
            uri: FakeResponse(200, b"new", {"content-type": "application/pdf"}),
        }
    )

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
        skip_existing=True,
        overwrite=False,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "SKIPPED_EXISTING_URI"
    assert rows[0]["already_existed"] == "True"
    assert len(session.calls) == 0
    assert existing.read_bytes() == b"existing"


def test_direct_document_downloader_http_error(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"
    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri)])

    session = FakeSession({uri: FakeResponse(404, b"not found", {"content-type": "text/plain"})})
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "HTTP_ERROR"
    assert rows[0]["http_status"] == "404"


def test_direct_document_downloader_request_exception(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"
    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri)])

    session = FakeSession({uri: RuntimeError("boom")})
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "REQUEST_EXCEPTION"
    assert rows[0]["error_type"] == "RuntimeError"


def test_direct_document_downloader_unexpected_content_type(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"
    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri)])

    session = FakeSession({uri: FakeResponse(200, b"<html></html>", {"content-type": "text/html"})})
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "UNEXPECTED_CONTENT_TYPE"
    assert rows[0]["saved_path"] == ""


def test_direct_document_downloader_dry_run(tmp_path: Path) -> None:
    uri = "https://example.com/downloadarquivo?id=1"
    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(uri)])

    session = FakeSession({uri: FakeResponse(200, b"%PDF", {"content-type": "application/pdf"})})
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
        dry_run=True,
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert rows[0]["download_status"] == "DRY_RUN"
    assert len(session.calls) == 0


def test_direct_document_downloader_relative_uri_with_base_url(tmp_path: Path) -> None:
    raw_uri = "/downloadarquivo?id=1"
    final_uri = "https://example.com/downloadarquivo?id=1"

    pdf_links = tmp_path / "pdf_links.csv"
    write_pdf_links(pdf_links, [link_row(raw_uri)])

    session = FakeSession({final_uri: FakeResponse(200, b"%PDF", {"content-type": "application/pdf"})})
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    downloader = DirectDocumentDownloader(
        pdf_links_path=pdf_links,
        landing_downloads_dir=tmp_path / "landing" / "downloads",
        output_dir=tmp_path / "raw" / "document_downloads",
        logger=logger,
        session=session,
        base_url="https://example.com",
    )

    outputs = downloader.run()
    rows = read_latest(Path(outputs["latest_csv_path"]))

    assert rows[0]["uri"] == final_uri
    assert rows[0]["download_status"] == "SUCCESS"
