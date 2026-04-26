from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class PdfTextRecord:
    run_id: str
    competencia: str
    source_path: str
    source_sha256: str
    page_number: int
    page_count: int
    text: str
    text_length: int
    extraction_status: str
    error_message: Optional[str]
    extracted_at_utc: str


class PdfTextExtractor:
    """
    Extrator RAW de texto dos PDFs inventariados.

    Entrada:
    - data/raw/source_inventory/source_inventory_latest.csv

    Saída:
    - data/raw/pdf_text/run_id=<RUN_ID>/pdf_text.csv
    - data/raw/pdf_text/run_id=<RUN_ID>/pdf_text.parquet, se pyarrow disponível
    - data/raw/pdf_text/pdf_text_latest.csv

    Regra:
    - não interpreta valores financeiros;
    - não cria Bronze;
    - apenas extrai texto bruto por página, preservando competência, caminho e hash.
    """

    MODULE = "floriano.raw.pdf_text_extractor"
    CLASS_NAME = "PdfTextExtractor"

    REQUIRED_INVENTORY_COLUMNS = {
        "competencia",
        "source_path",
        "sha256",
        "validation_status",
    }

    def __init__(
        self,
        source_inventory_path: str | Path = "data/raw/source_inventory/source_inventory_latest.csv",
        output_dir: str | Path = "data/raw/pdf_text",
        logger: Optional[PipelineExecutionLogger] = None,
        strict: bool = True,
        require_parquet: bool = False,
    ) -> None:
        self.source_inventory_path = Path(source_inventory_path)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.strict = strict
        self.require_parquet = require_parquet
        self.inventory_rows: List[Dict[str, str]] = []
        self.records: List[PdfTextRecord] = []

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def load_source_inventory(self) -> List[Dict[str, str]]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_source_inventory",
            operation="load_source_inventory",
            message="Carregando inventário de sources.",
            source_file=str(self.source_inventory_path),
        )

        if not self.source_inventory_path.exists():
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="load_source_inventory",
                operation="load_source_inventory",
                message="Arquivo de inventário não encontrado.",
                source_file=str(self.source_inventory_path),
                duration_ms=timer.elapsed_ms(),
            )
            raise FileNotFoundError(f"Inventário não encontrado: {self.source_inventory_path}")

        with self.source_inventory_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.inventory_rows = rows

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_source_inventory",
            operation="load_source_inventory",
            message="Inventário de sources carregado.",
            source_file=str(self.source_inventory_path),
            records_out=len(rows),
            duration_ms=timer.elapsed_ms(),
        )

        return rows

    def validate_inventory(self) -> List[Dict[str, str]]:
        timer = ExecutionTimer()

        if not self.inventory_rows:
            self.load_source_inventory()

        if not self.inventory_rows:
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="validate_inventory",
                operation="validate_inventory",
                message="Inventário vazio.",
                duration_ms=timer.elapsed_ms(),
            )
            raise ValueError("Inventário vazio.")

        available_columns = set(self.inventory_rows[0].keys())
        missing_columns = self.REQUIRED_INVENTORY_COLUMNS - available_columns

        if missing_columns:
            message = f"Inventário sem colunas obrigatórias: {sorted(missing_columns)}."
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="validate_inventory",
                operation="validate_inventory",
                message=message,
                duration_ms=timer.elapsed_ms(),
            )
            raise ValueError(message)

        invalid_rows = [
            row for row in self.inventory_rows
            if row.get("validation_status") != "VALID"
        ]

        if invalid_rows and self.strict:
            for row in invalid_rows:
                self.logger.failed(
                    layer="raw",
                    module=self.MODULE,
                    class_name=self.CLASS_NAME,
                    method_name="validate_inventory",
                    operation="validate_inventory_row",
                    message="Linha de inventário inválida.",
                    competencia=row.get("competencia"),
                    source_file=row.get("source_path"),
                )

            raise ValueError(
                f"Inventário contém {len(invalid_rows)} linha(s) não VALID."
            )

        valid_rows = [
            row for row in self.inventory_rows
            if row.get("validation_status") == "VALID"
        ]

        if not valid_rows:
            raise ValueError("Inventário não contém nenhuma linha VALID.")

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="validate_inventory",
            operation="validate_inventory",
            message="Inventário validado para extração de texto.",
            records_in=len(self.inventory_rows),
            records_out=len(valid_rows),
            duration_ms=timer.elapsed_ms(),
        )

        return valid_rows

    def extract_single_pdf(self, row: Dict[str, str]) -> List[PdfTextRecord]:
        timer = ExecutionTimer()

        competencia = row["competencia"]
        source_path = Path(row["source_path"])
        source_sha256 = row["sha256"]

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="extract_single_pdf",
            operation="extract_pdf_text",
            message="Iniciando extração de texto do PDF.",
            competencia=competencia,
            source_file=str(source_path),
            input_hash=source_sha256,
        )

        if not source_path.exists():
            message = f"PDF não encontrado: {source_path}"
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="extract_single_pdf",
                operation="extract_pdf_text",
                message=message,
                competencia=competencia,
                source_file=str(source_path),
                input_hash=source_sha256,
                duration_ms=timer.elapsed_ms(),
            )
            raise FileNotFoundError(message)

        try:
            import fitz
        except ImportError as exc:
            raise RuntimeError(
                "PyMuPDF não está instalado. Instale com: python -m pip install pymupdf"
            ) from exc

        records: List[PdfTextRecord] = []

        try:
            doc = fitz.open(source_path)
            page_count = len(doc)

            for idx, page in enumerate(doc, start=1):
                text = page.get_text("text") or ""

                records.append(
                    PdfTextRecord(
                        run_id=self.logger.run_id,
                        competencia=competencia,
                        source_path=str(source_path),
                        source_sha256=source_sha256,
                        page_number=idx,
                        page_count=page_count,
                        text=text,
                        text_length=len(text),
                        extraction_status="SUCCESS",
                        error_message=None,
                        extracted_at_utc=self._utc_now(),
                    )
                )

            doc.close()

            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="extract_single_pdf",
                operation="extract_pdf_text",
                message="Extração de texto do PDF concluída.",
                competencia=competencia,
                source_file=str(source_path),
                input_hash=source_sha256,
                records_out=len(records),
                duration_ms=timer.elapsed_ms(),
            )

            return records

        except Exception as exc:
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="extract_single_pdf",
                operation="extract_pdf_text",
                message="Falha na extração de texto do PDF.",
                competencia=competencia,
                source_file=str(source_path),
                input_hash=source_sha256,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                duration_ms=timer.elapsed_ms(),
            )

            if self.strict:
                raise

            return [
                PdfTextRecord(
                    run_id=self.logger.run_id,
                    competencia=competencia,
                    source_path=str(source_path),
                    source_sha256=source_sha256,
                    page_number=0,
                    page_count=0,
                    text="",
                    text_length=0,
                    extraction_status="FAILED",
                    error_message=str(exc),
                    extracted_at_utc=self._utc_now(),
                )
            ]

    def extract_pdf_text(self) -> List[PdfTextRecord]:
        timer = ExecutionTimer()

        rows = self.validate_inventory()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="extract_pdf_text",
            operation="extract_all_pdf_text",
            message="Iniciando extração de texto de todos os PDFs válidos.",
            records_in=len(rows),
        )

        all_records: List[PdfTextRecord] = []

        for row in rows:
            all_records.extend(self.extract_single_pdf(row))

        self.records = all_records

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="extract_pdf_text",
            operation="extract_all_pdf_text",
            message="Extração de texto de todos os PDFs concluída.",
            records_in=len(rows),
            records_out=len(all_records),
            duration_ms=timer.elapsed_ms(),
        )

        return all_records

    def build_text_records(self) -> List[Dict[str, object]]:
        if not self.records:
            self.extract_pdf_text()

        return [asdict(record) for record in self.records]

    def write_csv(self, rows: List[Dict[str, object]], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        if not rows:
            raise ValueError("Não há linhas para gravar em pdf_text.")

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        return path

    def write_parquet(self, rows: List[Dict[str, object]], path: Path) -> Optional[Path]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            if self.require_parquet:
                raise RuntimeError(
                    "pyarrow não está instalado. Instale com: python -m pip install pyarrow"
                ) from exc
            return None

        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)
        return path

    def write_output(self, rows: List[Dict[str, object]]) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        run_dir = self.output_dir / f"run_id={self.logger.run_id}"
        csv_path = run_dir / "pdf_text.csv"
        parquet_path = run_dir / "pdf_text.parquet"
        latest_csv_path = self.output_dir / "pdf_text_latest.csv"

        self.write_csv(rows, csv_path)
        self.write_csv(rows, latest_csv_path)
        parquet_written = self.write_parquet(rows, parquet_path)

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="write_output",
            operation="write_pdf_text",
            message="Texto bruto dos PDFs gravado.",
            records_out=len(rows),
            duration_ms=timer.elapsed_ms(),
            source_file=str(csv_path),
        )

        return {
            "csv_path": str(csv_path),
            "latest_csv_path": str(latest_csv_path),
            "parquet_path": str(parquet_written) if parquet_written else None,
        }

    def run(self) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="run",
            operation="pdf_text_extraction_pipeline",
            message="Pipeline de extração RAW de texto dos PDFs iniciado.",
        )

        try:
            rows = self.build_text_records()
            outputs = self.write_output(rows)

            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="pdf_text_extraction_pipeline",
                message="Pipeline de extração RAW de texto dos PDFs finalizado com sucesso.",
                records_out=len(rows),
                duration_ms=timer.elapsed_ms(),
            )

            self.logger.close(require_parquet=self.require_parquet)
            return outputs

        except Exception as exc:
            self.logger.failed(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="pdf_text_extraction_pipeline",
                message="Pipeline de extração RAW de texto dos PDFs falhou.",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                duration_ms=timer.elapsed_ms(),
            )
            self.logger.close(require_parquet=False)
            raise
