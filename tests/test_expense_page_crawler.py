from __future__ import annotations

import csv
from pathlib import Path

from floriano.raw.expense_page_crawler import ExpensePageCrawler
from floriano.utils.execution_logger import PipelineExecutionLogger


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        content: bytes,
        headers: dict[str, str] | None = None,
        url: str | None = None,
        history: list[object] | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.url = url or "https://example.com/despesa/1"
        self.history = history or []


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


def write_pdf_links(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
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
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def row(uri: str, link_type: str = "despesa") -> dict[str, str]:
    return {
        "run_id": "run-links",
        "competencia": "2026_03",
        "source_path": "source.pdf",
        "source_sha256": "sha-source",
        "page_number": "1",
        "uri": uri,
        "uri_sha256": ExpensePageCrawler.sha256_text(uri),
        "link_type": link_type,
    }


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_expense_page_crawler_dry_run(tmp_path: Path) -> None:
    uri = "https://example.com/despesa/1"
    links = tmp_path / "pdf_links.csv"
    write_pdf_links(links, [row(uri)])

    crawler = ExpensePageCrawler(
        pdf_links_path=links,
        landing_downloads_dir=tmp_path / "landing",
        output_dir=tmp_path / "raw" / "expense_pages",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=FakeSession({}),
        dry_run=True,
    )

    outputs = crawler.run()

    pages = read_csv(Path(outputs["expense_pages_latest_path"]))
    docs = read_csv(Path(outputs["expense_documents_latest_path"]))

    assert len(pages) == 1
    assert pages[0]["crawl_status"] == "DRY_RUN"
    assert docs == []


def test_expense_page_crawler_discovers_direct_pdf(tmp_path: Path) -> None:
    uri = "https://example.com/despesa/1"
    links = tmp_path / "pdf_links.csv"
    write_pdf_links(links, [row(uri)])

    session = FakeSession(
        {
            uri: FakeResponse(
                200,
                b"%PDF fake",
                {"content-type": "application/pdf"},
                url="https://example.com/comprovante.pdf",
            )
        }
    )

    crawler = ExpensePageCrawler(
        pdf_links_path=links,
        landing_downloads_dir=tmp_path / "landing",
        output_dir=tmp_path / "raw" / "expense_pages",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = crawler.run()

    pages = read_csv(Path(outputs["expense_pages_latest_path"]))
    docs = read_csv(Path(outputs["expense_documents_latest_path"]))

    assert pages[0]["crawl_status"] == "DIRECT_PDF_DISCOVERED"
    assert pages[0]["is_pdf_direct"] == "True"
    assert len(docs) == 1
    assert docs[0]["document_link_source"] == "direct_pdf_response"


def test_expense_page_crawler_discovers_documents_from_html(tmp_path: Path) -> None:
    uri = "https://example.com/despesa/1"
    links = tmp_path / "pdf_links.csv"
    write_pdf_links(links, [row(uri)])

    html = b"""
    <html>
      <body>
        <a href="/downloadarquivo?id=1">PDF 1</a>
        <a href="/downloadarquivo?id=2">PDF 2</a>
      </body>
    </html>
    """

    session = FakeSession(
        {
            uri: FakeResponse(
                200,
                html,
                {"content-type": "text/html; charset=utf-8"},
                url="https://example.com/despesa/1",
            )
        }
    )

    crawler = ExpensePageCrawler(
        pdf_links_path=links,
        landing_downloads_dir=tmp_path / "landing",
        output_dir=tmp_path / "raw" / "expense_pages",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = crawler.run()

    pages = read_csv(Path(outputs["expense_pages_latest_path"]))
    docs = read_csv(Path(outputs["expense_documents_latest_path"]))

    assert pages[0]["crawl_status"] == "HTML_PAGE_CRAWLED"
    assert pages[0]["is_html_page"] == "True"
    assert Path(pages[0]["html_saved_path"]).exists()
    assert len(docs) == 2


def test_expense_page_crawler_discovers_direct_image(tmp_path: Path) -> None:
    uri = "https://example.com/despesa/image"
    links = tmp_path / "pdf_links.csv"
    write_pdf_links(links, [row(uri)])

    session = FakeSession(
        {
            uri: FakeResponse(
                200,
                b"fake-jpeg-content",
                {"content-type": "image/jpeg"},
                url="https://example.com/comprovante.jpg",
            )
        }
    )

    crawler = ExpensePageCrawler(
        pdf_links_path=links,
        landing_downloads_dir=tmp_path / "landing",
        output_dir=tmp_path / "raw" / "expense_pages",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = crawler.run()

    pages = read_csv(Path(outputs["expense_pages_latest_path"]))
    docs = read_csv(Path(outputs["expense_documents_latest_path"]))

    assert len(pages) == 1
    assert pages[0]["crawl_status"] == "DIRECT_IMAGE_DISCOVERED"
    assert pages[0]["is_pdf_direct"] == "False"
    assert pages[0]["is_html_page"] == "False"
    assert len(docs) == 1
    assert docs[0]["document_link_source"] == "direct_image_response"
    assert docs[0]["document_type_hint"] == "direct_image"


def test_expense_page_crawler_discovers_embedded_html_documents(tmp_path: Path) -> None:
    uri = "https://example.com/despesa/html"
    links = tmp_path / "pdf_links.csv"
    write_pdf_links(links, [row(uri)])

    html = b"""
    <html>
      <body>
        <img src="/downloadarquivo?id=img1" alt="Imagem comprovante">
        <object data="/downloadarquivo?id=obj1"></object>
        <embed src="/downloadarquivo?id=emb1">
        <iframe src="/downloadarquivo?id=ifr1"></iframe>
        <img src="/assets/logo.png" alt="logo">
      </body>
    </html>
    """

    session = FakeSession(
        {
            uri: FakeResponse(
                200,
                html,
                {"content-type": "text/html; charset=utf-8"},
                url="https://example.com/despesa/html",
            )
        }
    )

    crawler = ExpensePageCrawler(
        pdf_links_path=links,
        landing_downloads_dir=tmp_path / "landing",
        output_dir=tmp_path / "raw" / "expense_pages",
        logger=PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test"),
        session=session,
    )

    outputs = crawler.run()

    pages = read_csv(Path(outputs["expense_pages_latest_path"]))
    docs = read_csv(Path(outputs["expense_documents_latest_path"]))

    sources = {r["document_link_source"] for r in docs}

    assert pages[0]["crawl_status"] == "HTML_PAGE_CRAWLED"
    assert pages[0]["is_html_page"] == "True"
    assert len(docs) == 4
    assert "html_img_src" in sources
    assert "html_object_data" in sources
    assert "html_embed_src" in sources
    assert "html_iframe_src" in sources
