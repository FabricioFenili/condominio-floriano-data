from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from floriano.raw.document_download_consistency import DocumentDownloadConsistencyChecker
from floriano.utils.execution_logger import PipelineExecutionLogger


DOWNLOAD_FIELDS = [
    "run_id",
    "upstream_pdf_links_run_id",
    "competencia",
    "source_pdf_path",
    "source_pdf_sha256",
    "page_number",
    "uri",
    "uri_sha256",
    "link_type",
    "duplicate_uri_count",
    "download_attempted",
    "download_status",
    "http_status",
    "content_type",
    "content_disposition",
    "file_name_detected",
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
    "downloaded_at_utc",
]

LINK_FIELDS = [
    "run_id",
    "competencia",
    "source_path",
    "source_sha256",
    "page_number",
    "uri",
    "uri_sha256",
    "link_type",
]


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def make_file(path: Path, content: bytes = b"%PDF fake") -> tuple[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return sha256_bytes(content), len(content)


def link_row(uri_sha: str = "uri-1", comp: str = "2026_03", link_type: str = "downloadarquivo") -> dict[str, str]:
    return {
        "run_id": "run-links",
        "competencia": comp,
        "source_path": "source.pdf",
        "source_sha256": "source-sha",
        "page_number": "1",
        "uri": f"https://example.com/{uri_sha}",
        "uri_sha256": uri_sha,
        "link_type": link_type,
    }


def download_row(
    saved_path: Path,
    file_sha: str,
    file_size: int,
    uri_sha: str = "uri-1",
    comp: str = "2026_03",
    status: str = "SUCCESS",
    conflict: str = "False",
    run_id: str = "run-downloads",
) -> dict[str, str]:
    return {
        "run_id": run_id,
        "upstream_pdf_links_run_id": "run-links",
        "competencia": comp,
        "source_pdf_path": "source.pdf",
        "source_pdf_sha256": "source-sha",
        "page_number": "1",
        "uri": f"https://example.com/{uri_sha}",
        "uri_sha256": uri_sha,
        "link_type": "downloadarquivo",
        "duplicate_uri_count": "1",
        "download_attempted": "True",
        "download_status": status,
        "http_status": "200",
        "content_type": "application/pdf",
        "content_disposition": "",
        "file_name_detected": saved_path.name,
        "saved_path": str(saved_path),
        "file_size_bytes": str(file_size),
        "file_sha256": file_sha,
        "already_existed": "False",
        "was_downloaded_now": "True",
        "is_duplicate_uri": "False",
        "is_duplicate_file": "False",
        "conflict_detected": conflict,
        "error_type": "",
        "error_message": "",
        "downloaded_at_utc": "2026-04-26T00:00:00+00:00",
    }


def make_checker(tmp_path: Path, downloads: Path, links: Path) -> DocumentDownloadConsistencyChecker:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-check")

    return DocumentDownloadConsistencyChecker(
        document_downloads_path=downloads,
        pdf_links_path=links,
        output_dir=tmp_path / "quality",
        latest_registry_dir=tmp_path / "_latest",
        manifest_dir=tmp_path / "_manifests",
        logger=logger,
        require_parquet=False,
    )


def test_document_download_consistency_success(tmp_path: Path) -> None:
    file_path = tmp_path / "landing" / "downloads" / "2026_03" / "comprovantes" / "uri-1.pdf"
    file_sha, file_size = make_file(file_path)

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, file_sha, file_size)])
    write_csv(links, LINK_FIELDS, [link_row()])

    checker = make_checker(tmp_path, downloads, links)
    outputs = checker.run()

    assert Path(outputs["latest_csv_path"]).exists()
    assert Path(outputs["latest_summary_path"]).exists()
    assert Path(outputs["manifest_path"]).exists()

    summary = Path(outputs["latest_summary_path"]).read_text(encoding="utf-8")
    assert '"approved": true' in summary


def test_document_download_consistency_fails_on_missing_file(tmp_path: Path) -> None:
    file_path = tmp_path / "missing.pdf"

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, "fake-sha", 10)])
    write_csv(links, LINK_FIELDS, [link_row()])

    checker = make_checker(tmp_path, downloads, links)

    with pytest.raises(ValueError):
        checker.run()


def test_document_download_consistency_fails_on_hash_mismatch(tmp_path: Path) -> None:
    file_path = tmp_path / "file.pdf"
    _, file_size = make_file(file_path, b"abc")

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, "wrong-hash", file_size)])
    write_csv(links, LINK_FIELDS, [link_row()])

    checker = make_checker(tmp_path, downloads, links)

    with pytest.raises(ValueError):
        checker.run()


def test_document_download_consistency_fails_on_conflict(tmp_path: Path) -> None:
    file_path = tmp_path / "file.pdf"
    file_sha, file_size = make_file(file_path)

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, file_sha, file_size, conflict="True")])
    write_csv(links, LINK_FIELDS, [link_row()])

    checker = make_checker(tmp_path, downloads, links)

    with pytest.raises(ValueError):
        checker.run()


def test_document_download_consistency_fails_on_bad_status(tmp_path: Path) -> None:
    file_path = tmp_path / "file.pdf"
    file_sha, file_size = make_file(file_path)

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, file_sha, file_size, status="HTTP_ERROR")])
    write_csv(links, LINK_FIELDS, [link_row()])

    checker = make_checker(tmp_path, downloads, links)

    with pytest.raises(ValueError):
        checker.run()


def test_document_download_consistency_allows_duplicate_file_sha(tmp_path: Path) -> None:
    file_path = tmp_path / "file.pdf"
    file_sha, file_size = make_file(file_path)

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, file_sha, file_size, status="DUPLICATE_FILE_SHA")])
    write_csv(links, LINK_FIELDS, [link_row()])

    checker = make_checker(tmp_path, downloads, links)

    outputs = checker.run()
    assert Path(outputs["latest_summary_path"]).exists()


def test_document_download_consistency_fails_on_uri_outside_pdf_links(tmp_path: Path) -> None:
    file_path = tmp_path / "file.pdf"
    file_sha, file_size = make_file(file_path)

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(downloads, DOWNLOAD_FIELDS, [download_row(file_path, file_sha, file_size, uri_sha="uri-outside")])
    write_csv(links, LINK_FIELDS, [link_row(uri_sha="uri-1")])

    checker = make_checker(tmp_path, downloads, links)

    with pytest.raises(ValueError):
        checker.run()


def test_document_download_consistency_fails_on_multiple_run_ids(tmp_path: Path) -> None:
    file_a = tmp_path / "a.pdf"
    file_b = tmp_path / "b.pdf"
    sha_a, size_a = make_file(file_a, b"a")
    sha_b, size_b = make_file(file_b, b"b")

    downloads = tmp_path / "document_downloads.csv"
    links = tmp_path / "pdf_links.csv"

    write_csv(
        downloads,
        DOWNLOAD_FIELDS,
        [
            download_row(file_a, sha_a, size_a, uri_sha="uri-1", run_id="run-a"),
            download_row(file_b, sha_b, size_b, uri_sha="uri-2", run_id="run-b"),
        ],
    )
    write_csv(links, LINK_FIELDS, [link_row(uri_sha="uri-1"), link_row(uri_sha="uri-2")])

    checker = make_checker(tmp_path, downloads, links)

    with pytest.raises(ValueError):
        checker.run()
