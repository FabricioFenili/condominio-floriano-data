from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from floriano.utils.competence import CompetenceRangeResolver
from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class SourceInventoryRecord:
    run_id: str
    competencia: str
    expected_file_name: str
    source_path: str
    exists: bool
    is_file: bool
    is_empty: bool
    file_size_bytes: int
    sha256: Optional[str]
    source_type: str
    collection_mode: str
    validation_status: str
    validation_message: str
    discovered_at_utc: str


class SourceInventoryBuilder:
    MODULE = "floriano.raw.source_inventory"
    CLASS_NAME = "SourceInventoryBuilder"

    def __init__(
        self,
        manual_upload_dir: str | Path = "data/manual_upload",
        output_dir: str | Path = "data/raw/source_inventory",
        expected_months: Optional[Iterable[str]] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        month: Optional[str] = None,
        default_start_month: str = "2025_05",
        as_of_date: Optional[str] = None,
        logger: Optional[PipelineExecutionLogger] = None,
        strict: bool = True,
        require_parquet: bool = False,
    ) -> None:
        self.manual_upload_dir = Path(manual_upload_dir)
        self.output_dir = Path(output_dir)

        resolver = CompetenceRangeResolver(
            default_start_month=default_start_month,
            as_of_date=as_of_date,
        )

        resolved_range = resolver.resolve(
            month=month,
            start_month=start_month,
            end_month=end_month,
        )

        self.expected_months = list(expected_months) if expected_months else resolved_range.months
        self.competence_range = resolved_range
        self.logger = logger or PipelineExecutionLogger()
        self.strict = strict
        self.require_parquet = require_parquet
        self.records: List[SourceInventoryRecord] = []

    @staticmethod
    def build_month_range(start_month: str, end_month: str) -> List[str]:
        return CompetenceRangeResolver.build_month_range(start_month, end_month)

    @staticmethod
    def expected_pdf_name(competencia: str) -> str:
        return f"prestacao_contas_{competencia}_print.pdf"

    def expected_pdf_path(self, competencia: str) -> Path:
        return (
            self.manual_upload_dir
            / competencia
            / "source"
            / self.expected_pdf_name(competencia)
        )

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def calculate_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _record_for_month(self, competencia: str) -> SourceInventoryRecord:
        path = self.expected_pdf_path(competencia)
        exists = path.exists()
        is_file = path.is_file()
        size = path.stat().st_size if exists and is_file else 0
        is_empty = exists and is_file and size == 0
        sha256 = self.calculate_sha256(path) if exists and is_file and not is_empty else None

        if not exists:
            status = "MISSING"
            message = "Arquivo source esperado não encontrado."
        elif not is_file:
            status = "INVALID"
            message = "Caminho existe, mas não é arquivo."
        elif is_empty:
            status = "EMPTY"
            message = "Arquivo source existe, mas está vazio."
        else:
            status = "VALID"
            message = "Arquivo source encontrado, não vazio e com hash calculado."

        return SourceInventoryRecord(
            run_id=self.logger.run_id,
            competencia=competencia,
            expected_file_name=self.expected_pdf_name(competencia),
            source_path=str(path),
            exists=exists,
            is_file=is_file,
            is_empty=is_empty,
            file_size_bytes=size,
            sha256=sha256,
            source_type="prestacao_contas_pdf",
            collection_mode="manual_print_pdf",
            validation_status=status,
            validation_message=message,
            discovered_at_utc=self._utc_now(),
        )

    def discover_sources(self) -> List[SourceInventoryRecord]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="discover_sources",
            operation="discover_expected_sources",
            message="Iniciando descoberta de arquivos source esperados.",
            records_in=len(self.expected_months),
        )

        self.records = [self._record_for_month(month) for month in self.expected_months]
        valid_count = sum(1 for r in self.records if r.validation_status == "VALID")

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="discover_sources",
            operation="discover_expected_sources",
            message="Descoberta de arquivos source concluída.",
            records_in=len(self.expected_months),
            records_out=len(self.records),
            duration_ms=timer.elapsed_ms(),
        )

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="discover_sources",
            operation="valid_sources_count",
            message=f"Arquivos válidos encontrados: {valid_count}/{len(self.records)}.",
            records_out=valid_count,
        )

        return self.records

    def validate_sources(self) -> None:
        timer = ExecutionTimer()

        if not self.records:
            self.discover_sources()

        invalid = [r for r in self.records if r.validation_status != "VALID"]

        if invalid:
            for rec in invalid:
                self.logger.warning(
                    layer="raw",
                    module=self.MODULE,
                    class_name=self.CLASS_NAME,
                    method_name="validate_sources",
                    operation="validate_source",
                    message=rec.validation_message,
                    competencia=rec.competencia,
                    source_file=rec.source_path,
                )

            if self.strict:
                self.logger.failed(
                    layer="raw",
                    module=self.MODULE,
                    class_name=self.CLASS_NAME,
                    method_name="validate_sources",
                    operation="validate_sources",
                    message=f"Validação falhou: {len(invalid)} arquivo(s) inválido(s).",
                    records_in=len(self.records),
                    records_out=len(invalid),
                    duration_ms=timer.elapsed_ms(),
                )
                raise ValueError(
                    f"Source inventory inválido: {len(invalid)} competência(s) com problema."
                )

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="validate_sources",
            operation="validate_sources",
            message="Validação dos arquivos source concluída.",
            records_in=len(self.records),
            records_out=len(self.records) - len(invalid),
            duration_ms=timer.elapsed_ms(),
        )

    def build_inventory(self) -> List[Dict[str, object]]:
        if not self.records:
            self.discover_sources()
        self.validate_sources()
        return [asdict(record) for record in self.records]

    def write_csv(self, rows: List[Dict[str, object]], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        if not rows:
            raise ValueError("Não há linhas para gravar no inventário.")

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
        csv_path = run_dir / "source_inventory.csv"
        parquet_path = run_dir / "source_inventory.parquet"
        latest_csv_path = self.output_dir / "source_inventory_latest.csv"

        self.write_csv(rows, csv_path)
        self.write_csv(rows, latest_csv_path)
        parquet_written = self.write_parquet(rows, parquet_path)

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="write_output",
            operation="write_source_inventory",
            message="Inventário de sources gravado.",
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
            operation="source_inventory_pipeline",
            message="Pipeline de inventário de sources iniciado.",
            records_in=len(self.expected_months),
        )

        try:
            rows = self.build_inventory()
            outputs = self.write_output(rows)

            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="source_inventory_pipeline",
                message="Pipeline de inventário de sources finalizado com sucesso.",
                records_in=len(self.expected_months),
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
                operation="source_inventory_pipeline",
                message="Pipeline de inventário de sources falhou.",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                duration_ms=timer.elapsed_ms(),
            )
            self.logger.close(require_parquet=False)
            raise
