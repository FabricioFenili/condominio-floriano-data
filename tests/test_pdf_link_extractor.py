from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from floriano.raw.pdf_link_extractor import PdfLinkExtractor
from floriano.utils.execution_logger import PipelineExecutionLogger


def create_pdf_with_links(path: Path, links: list[str]) -> None:
    import fitz

    path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()
    page = doc.new_page()

    for idx, uri in enumerate(links):
        y = 72 + (idx * 30)
        text = f"Link {idx + 1}"
        page.insert_text((72, y), text)
        rect = fitz.Rect(72, y - 12, 180, y + 8)
        page.insert_link({
            "kind": fitz.LINK_URI,
            "from": rect,
            "uri": uri,
        })

    doc.save(path)
    doc.close()


def create_pdf_without_links(path: Path) -> None:
    import fitz

    path.parent.mkdir(parents=True, exist_ok=True)

    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Sem links")
    doc.save(path)
    doc.close()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_inventory(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "run_id",
        "competencia",
        "expected_file_name",
        "source_path",
        "exists",
        "is_file",
        "is_empty",
        "file_size_bytes",
        "sha256",
        "source_type",
        "collection_mode",
        "validation_status",
        "validation_message",
        "discovered_at_utc",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def inventory_row(pdf_path: Path, competencia: str = "2026_03", status: str = "VALID") -> dict[str, str]:
    return {
        "run_id": "run-inventory-test",
        "competencia": competencia,
        "expected_file_name": pdf_path.name,
        "source_path": str(pdf_path),
        "exists": "True",
        "is_file": "True",
        "is_empty": "False",
        "file_size_bytes": str(pdf_path.stat().st_size if pdf_path.exists() else 0),
        "sha256": sha256(pdf_path) if pdf_path.exists() else "",
        "source_type": "prestacao_contas_pdf",
        "collection_mode": "manual_print_pdf",
        "validation_status": status,
        "validation_message": "ok",
        "discovered_at_utc": "2026-04-26T00:00:00+00:00",
    }


def test_pdf_link_extractor_success(tmp_path: Path) -> None:
    pdf_path = tmp_path / "manual_upload" / "2026_03" / "source" / "prestacao_contas_2026_03_print.pdf"

    create_pdf_with_links(
        pdf_path,
        [
            "https://example.com/downloadarquivo?id=1",
            "https://example.com/despesas/123",
        ],
    )

    inventory_path = tmp_path / "raw" / "source_inventory" / "source_inventory_latest.csv"
    write_inventory(inventory_path, [inventory_row(pdf_path)])

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    extractor = PdfLinkExtractor(
        source_inventory_path=inventory_path,
        output_dir=tmp_path / "raw" / "pdf_links",
        logger=logger,
        strict=True,
    )

    outputs = extractor.run()

    latest_csv = Path(outputs["latest_csv_path"])
    assert latest_csv.exists()

    with latest_csv.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    assert rows[0]["competencia"] == "2026_03"
    assert rows[0]["link_kind"] == "LINK_URI"
    assert rows[0]["link_type"] == "downloadarquivo"
    assert rows[1]["link_type"] == "despesa"
    assert rows[0]["extraction_status"] == "SUCCESS"


def test_pdf_link_extractor_handles_pdf_without_links(tmp_path: Path) -> None:
    pdf_path = tmp_path / "source.pdf"
    create_pdf_without_links(pdf_path)

    inventory_path = tmp_path / "inventory.csv"
    write_inventory(inventory_path, [inventory_row(pdf_path)])

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    extractor = PdfLinkExtractor(
        source_inventory_path=inventory_path,
        output_dir=tmp_path / "raw" / "pdf_links",
        logger=logger,
        strict=True,
    )

    outputs = extractor.run()

    with Path(outputs["latest_csv_path"]).open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows == []


def test_pdf_link_extractor_missing_inventory_fails(tmp_path: Path) -> None:
    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    extractor = PdfLinkExtractor(
        source_inventory_path=tmp_path / "missing.csv",
        output_dir=tmp_path / "raw" / "pdf_links",
        logger=logger,
        strict=True,
    )

    with pytest.raises(FileNotFoundError):
        extractor.run()


def test_pdf_link_extractor_invalid_inventory_status_fails_in_strict_mode(tmp_path: Path) -> None:
    pdf_path = tmp_path / "source.pdf"
    create_pdf_with_links(pdf_path, ["https://example.com/downloadarquivo?id=1"])

    inventory_path = tmp_path / "inventory.csv"
    write_inventory(inventory_path, [inventory_row(pdf_path, status="MISSING")])

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    extractor = PdfLinkExtractor(
        source_inventory_path=inventory_path,
        output_dir=tmp_path / "raw" / "pdf_links",
        logger=logger,
        strict=True,
    )

    with pytest.raises(ValueError):
        extractor.run()


def test_pdf_link_extractor_filters_invalid_inventory_in_non_strict_mode(tmp_path: Path) -> None:
    valid_pdf = tmp_path / "valid.pdf"
    invalid_pdf = tmp_path / "invalid.pdf"

    create_pdf_with_links(valid_pdf, ["https://example.com/downloadarquivo?id=1"])
    create_pdf_with_links(invalid_pdf, ["https://example.com/downloadarquivo?id=2"])

    inventory_path = tmp_path / "inventory.csv"
    write_inventory(
        inventory_path,
        [
            inventory_row(valid_pdf, competencia="2026_03", status="VALID"),
            inventory_row(invalid_pdf, competencia="2026_04", status="MISSING"),
        ],
    )

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    extractor = PdfLinkExtractor(
        source_inventory_path=inventory_path,
        output_dir=tmp_path / "raw" / "pdf_links",
        logger=logger,
        strict=False,
    )

    outputs = extractor.run()

    with Path(outputs["latest_csv_path"]).open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert rows[0]["competencia"] == "2026_03"


def test_pdf_link_extractor_missing_pdf_after_inventory_fails(tmp_path: Path) -> None:
    pdf_path = tmp_path / "missing.pdf"

    inventory_path = tmp_path / "inventory.csv"
    write_inventory(inventory_path, [inventory_row(pdf_path, status="VALID")])

    logger = PipelineExecutionLogger(base_dir=tmp_path / "logs", run_id="run-test")

    extractor = PdfLinkExtractor(
        source_inventory_path=inventory_path,
        output_dir=tmp_path / "raw" / "pdf_links",
        logger=logger,
        strict=True,
    )

    with pytest.raises(FileNotFoundError):
        extractor.run()


def test_classify_link() -> None:
    assert PdfLinkExtractor.classify_link("https://x.com/downloadarquivo?id=1") == "downloadarquivo"
    assert PdfLinkExtractor.classify_link("https://x.com/despesas/1") == "despesa"
    assert PdfLinkExtractor.classify_link("https://x.com/arquivos/1") == "arquivo"
    assert PdfLinkExtractor.classify_link("/clients/areadocondomino/relatorios") == "relatorio"
    assert PdfLinkExtractor.classify_link("mailto:test@example.com") == "email"
    assert PdfLinkExtractor.classify_link("") == "empty"
