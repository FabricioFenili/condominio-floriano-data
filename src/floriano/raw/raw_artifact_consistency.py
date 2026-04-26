from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from floriano.utils.artifacts import (
    ArtifactManifestManager,
    LatestArtifactRegistry,
    sha256_file,
)
from floriano.utils.execution_logger import ExecutionTimer, PipelineExecutionLogger


@dataclass(frozen=True)
class RawConsistencyRecord:
    run_id: str
    check_name: str
    artifact_name: str
    severity: str
    status: str
    message: str
    details_json: str
    checked_at_utc: str


class RawArtifactConsistencyChecker:
    """
    Gate transversal de consistência da camada RAW.

    Valida:
    - latest com run_id único por artefato;
    - source_inventory sem competências inválidas;
    - pdf_text cobrindo todas as competências válidas;
    - pdf_text sem página duplicada;
    - pdf_links sem competências fora do inventário;
    - consistência de source_sha256 entre source, text e links;
    - manifesto e latest registry dos artefatos RAW.

    Este módulo não interpreta finanças.
    """

    MODULE = "floriano.raw.raw_artifact_consistency"
    CLASS_NAME = "RawArtifactConsistencyChecker"

    SOURCE_REQUIRED = {"run_id", "competencia", "source_path", "sha256", "validation_status"}
    TEXT_REQUIRED = {"run_id", "competencia", "source_path", "source_sha256", "page_number", "page_count", "text_length"}
    LINKS_REQUIRED = {"run_id", "competencia", "source_path", "source_sha256", "page_number", "uri_sha256", "link_type"}

    def __init__(
        self,
        source_inventory_path: str | Path = "data/raw/source_inventory/source_inventory_latest.csv",
        pdf_text_path: str | Path = "data/raw/pdf_text/pdf_text_latest.csv",
        pdf_links_path: str | Path = "data/raw/pdf_links/pdf_links_latest.csv",
        output_dir: str | Path = "data/raw/quality",
        latest_registry_dir: str | Path = "data/raw/_latest",
        manifest_dir: str | Path = "data/raw/_manifests",
        logger: Optional[PipelineExecutionLogger] = None,
        require_parquet: bool = False,
        fail_on_warning: bool = False,
    ) -> None:
        self.source_inventory_path = Path(source_inventory_path)
        self.pdf_text_path = Path(pdf_text_path)
        self.pdf_links_path = Path(pdf_links_path)
        self.output_dir = Path(output_dir)
        self.logger = logger or PipelineExecutionLogger()
        self.require_parquet = require_parquet
        self.fail_on_warning = fail_on_warning

        self.registry = LatestArtifactRegistry(base_dir=latest_registry_dir)
        self.manifest_manager = ArtifactManifestManager(base_dir=manifest_dir)

        self.source_rows: List[Dict[str, str]] = []
        self.text_rows: List[Dict[str, str]] = []
        self.link_rows: List[Dict[str, str]] = []
        self.records: List[RawConsistencyRecord] = []

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    @staticmethod
    def _load_csv(path: Path, *, allow_empty: bool = False) -> Tuple[List[Dict[str, str]], List[str]]:
        if not path.exists():
            raise FileNotFoundError(f"CSV não encontrado: {path}")

        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])

        if not rows and not allow_empty:
            raise ValueError(f"CSV vazio: {path}")

        return rows, fieldnames

    def _add(
        self,
        *,
        check_name: str,
        artifact_name: str,
        severity: str,
        status: str,
        message: str,
        details: Optional[Dict[str, object]] = None,
    ) -> None:
        self.records.append(
            RawConsistencyRecord(
                run_id=self.logger.run_id,
                check_name=check_name,
                artifact_name=artifact_name,
                severity=severity,
                status=status,
                message=message,
                details_json=json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                checked_at_utc=self._utc_now(),
            )
        )

    @staticmethod
    def _single_run_id(rows: List[Dict[str, str]]) -> Optional[str]:
        run_ids = sorted({r.get("run_id", "") for r in rows if r.get("run_id")})
        if len(run_ids) == 1:
            return run_ids[0]
        return None

    @staticmethod
    def _record_count(path: Path, allow_empty: bool = False) -> int:
        rows, _ = RawArtifactConsistencyChecker._load_csv(path, allow_empty=allow_empty)
        return len(rows)

    def load_inputs(self) -> None:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_inputs",
            operation="load_raw_latest_artifacts",
            message="Carregando artefatos latest da RAW.",
        )

        self.source_rows, source_cols = self._load_csv(self.source_inventory_path)
        self.text_rows, text_cols = self._load_csv(self.pdf_text_path)
        self.link_rows, link_cols = self._load_csv(self.pdf_links_path, allow_empty=True)

        missing_source = self.SOURCE_REQUIRED - set(source_cols)
        missing_text = self.TEXT_REQUIRED - set(text_cols)
        missing_links = self.LINKS_REQUIRED - set(link_cols)

        if missing_source:
            raise ValueError(f"source_inventory sem colunas obrigatórias: {sorted(missing_source)}")

        if missing_text:
            raise ValueError(f"pdf_text sem colunas obrigatórias: {sorted(missing_text)}")

        if missing_links:
            raise ValueError(f"pdf_links sem colunas obrigatórias: {sorted(missing_links)}")

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="load_inputs",
            operation="load_raw_latest_artifacts",
            message="Artefatos RAW carregados.",
            records_out=len(self.source_rows) + len(self.text_rows) + len(self.link_rows),
            duration_ms=timer.elapsed_ms(),
        )

    def check_latest_single_run_id(self) -> None:
        artifacts = [
            ("source_inventory", self.source_rows),
            ("pdf_text", self.text_rows),
            ("pdf_links", self.link_rows),
        ]

        for artifact_name, rows in artifacts:
            run_ids = sorted({r.get("run_id", "") for r in rows if r.get("run_id")})

            if not rows and artifact_name == "pdf_links":
                self._add(
                    check_name="latest_single_run_id",
                    artifact_name=artifact_name,
                    severity="WARNING",
                    status="WARN",
                    message="pdf_links está vazio; não há run_id a validar.",
                    details={"run_ids": run_ids},
                )
                continue

            if len(run_ids) == 1:
                self._add(
                    check_name="latest_single_run_id",
                    artifact_name=artifact_name,
                    severity="INFO",
                    status="PASS",
                    message="Artefato latest contém exatamente um run_id.",
                    details={"run_ids": run_ids},
                )
            else:
                self._add(
                    check_name="latest_single_run_id",
                    artifact_name=artifact_name,
                    severity="CRITICAL",
                    status="FAIL",
                    message="Artefato latest contém zero ou múltiplos run_id.",
                    details={"run_ids": run_ids},
                )

    def check_source_inventory(self) -> Dict[str, str]:
        invalid = [r for r in self.source_rows if r["validation_status"] != "VALID"]
        competencias = [r["competencia"] for r in self.source_rows]
        dup_competencias = sorted([k for k, v in Counter(competencias).items() if v > 1])

        source_map = {r["competencia"]: r["sha256"] for r in self.source_rows if r["validation_status"] == "VALID"}

        if invalid:
            self._add(
                check_name="source_inventory_all_valid",
                artifact_name="source_inventory",
                severity="CRITICAL",
                status="FAIL",
                message="source_inventory contém linhas não VALID.",
                details={"invalid_count": len(invalid), "invalid_competencias": [r["competencia"] for r in invalid]},
            )
        else:
            self._add(
                check_name="source_inventory_all_valid",
                artifact_name="source_inventory",
                severity="INFO",
                status="PASS",
                message="Todas as linhas do source_inventory estão VALID.",
                details={"row_count": len(self.source_rows)},
            )

        if dup_competencias:
            self._add(
                check_name="source_inventory_unique_competencia",
                artifact_name="source_inventory",
                severity="CRITICAL",
                status="FAIL",
                message="source_inventory contém competências duplicadas.",
                details={"duplicated_competencias": dup_competencias},
            )
        else:
            self._add(
                check_name="source_inventory_unique_competencia",
                artifact_name="source_inventory",
                severity="INFO",
                status="PASS",
                message="source_inventory não contém competências duplicadas.",
                details={"competencias": sorted(source_map.keys())},
            )

        return source_map

    def check_pdf_text(self, source_map: Dict[str, str]) -> None:
        expected_competencias = set(source_map.keys())
        text_competencias = {r["competencia"] for r in self.text_rows}

        missing = sorted(expected_competencias - text_competencias)
        unexpected = sorted(text_competencias - expected_competencias)

        if missing or unexpected:
            self._add(
                check_name="pdf_text_competencia_coverage",
                artifact_name="pdf_text",
                severity="CRITICAL",
                status="FAIL",
                message="pdf_text não cobre exatamente as competências VALID do source_inventory.",
                details={"missing": missing, "unexpected": unexpected},
            )
        else:
            self._add(
                check_name="pdf_text_competencia_coverage",
                artifact_name="pdf_text",
                severity="INFO",
                status="PASS",
                message="pdf_text cobre todas as competências VALID do source_inventory.",
                details={"competencias": sorted(text_competencias)},
            )

        key_counter = Counter(
            (
                r["competencia"],
                r["source_sha256"],
                r["page_number"],
            )
            for r in self.text_rows
        )
        duplicates = [k for k, v in key_counter.items() if v > 1]

        if duplicates:
            self._add(
                check_name="pdf_text_no_duplicate_pages",
                artifact_name="pdf_text",
                severity="CRITICAL",
                status="FAIL",
                message="pdf_text contém páginas duplicadas.",
                details={"duplicate_count": len(duplicates), "sample": [str(x) for x in duplicates[:10]]},
            )
        else:
            self._add(
                check_name="pdf_text_no_duplicate_pages",
                artifact_name="pdf_text",
                severity="INFO",
                status="PASS",
                message="pdf_text não contém páginas duplicadas.",
                details={},
            )

        text_length_by_comp = defaultdict(int)
        pages_by_comp = defaultdict(set)
        page_count_by_comp = defaultdict(set)

        for r in self.text_rows:
            comp = r["competencia"]
            text_length_by_comp[comp] += int(float(r.get("text_length") or 0))
            pages_by_comp[comp].add(int(float(r.get("page_number") or 0)))
            page_count_by_comp[comp].add(int(float(r.get("page_count") or 0)))

        zero_text = sorted([comp for comp, total in text_length_by_comp.items() if total <= 0])
        invalid_page_counts = sorted([comp for comp, counts in page_count_by_comp.items() if len(counts) != 1 or list(counts)[0] <= 0])

        if zero_text or invalid_page_counts:
            self._add(
                check_name="pdf_text_content_quality",
                artifact_name="pdf_text",
                severity="CRITICAL",
                status="FAIL",
                message="pdf_text contém competência sem texto ou com page_count inválido.",
                details={"zero_text": zero_text, "invalid_page_counts": invalid_page_counts},
            )
        else:
            self._add(
                check_name="pdf_text_content_quality",
                artifact_name="pdf_text",
                severity="INFO",
                status="PASS",
                message="pdf_text contém texto e page_count válido por competência.",
                details={"text_length_by_competencia": dict(sorted(text_length_by_comp.items()))},
            )

    def check_pdf_links(self, source_map: Dict[str, str]) -> None:
        if not self.link_rows:
            self._add(
                check_name="pdf_links_non_empty",
                artifact_name="pdf_links",
                severity="WARNING",
                status="WARN",
                message="pdf_links está vazio. Pode ser legítimo, mas reduz capacidade de conciliação documental.",
                details={},
            )
            return

        expected_competencias = set(source_map.keys())
        link_competencias = {r["competencia"] for r in self.link_rows}
        unexpected = sorted(link_competencias - expected_competencias)
        without_links = sorted(expected_competencias - link_competencias)

        if unexpected:
            self._add(
                check_name="pdf_links_competencia_scope",
                artifact_name="pdf_links",
                severity="CRITICAL",
                status="FAIL",
                message="pdf_links contém competência fora do source_inventory.",
                details={"unexpected": unexpected},
            )
        else:
            self._add(
                check_name="pdf_links_competencia_scope",
                artifact_name="pdf_links",
                severity="INFO",
                status="PASS",
                message="pdf_links contém apenas competências presentes no source_inventory.",
                details={"competencias_with_links": sorted(link_competencias)},
            )

        if without_links:
            self._add(
                check_name="pdf_links_missing_competencias",
                artifact_name="pdf_links",
                severity="WARNING",
                status="WARN",
                message="Há competências sem links extraídos.",
                details={"competencias_without_links": without_links},
            )
        else:
            self._add(
                check_name="pdf_links_missing_competencias",
                artifact_name="pdf_links",
                severity="INFO",
                status="PASS",
                message="Todas as competências têm pelo menos um link extraído.",
                details={},
            )

        exact_key_counter = Counter(
            (
                r["competencia"],
                r["source_sha256"],
                r["page_number"],
                r["uri_sha256"],
                r.get("rect_x0", ""),
                r.get("rect_y0", ""),
                r.get("rect_x1", ""),
                r.get("rect_y1", ""),
            )
            for r in self.link_rows
        )
        exact_duplicates = [k for k, v in exact_key_counter.items() if v > 1]

        if exact_duplicates:
            self._add(
                check_name="pdf_links_no_exact_duplicates",
                artifact_name="pdf_links",
                severity="CRITICAL",
                status="FAIL",
                message="pdf_links contém duplicatas exatas.",
                details={"duplicate_count": len(exact_duplicates), "sample": [str(x) for x in exact_duplicates[:10]]},
            )
        else:
            self._add(
                check_name="pdf_links_no_exact_duplicates",
                artifact_name="pdf_links",
                severity="INFO",
                status="PASS",
                message="pdf_links não contém duplicatas exatas.",
                details={},
            )

        uri_counter = Counter(r["uri_sha256"] for r in self.link_rows if r.get("uri_sha256"))
        duplicate_uris = [k for k, v in uri_counter.items() if v > 1]

        if duplicate_uris:
            self._add(
                check_name="pdf_links_duplicate_uri_warning",
                artifact_name="pdf_links",
                severity="WARNING",
                status="WARN",
                message="Há URI repetida em pdf_links. Isso pode ser legítimo, mas deve ser deduplicado no downloader.",
                details={"duplicate_uri_count": len(duplicate_uris)},
            )
        else:
            self._add(
                check_name="pdf_links_duplicate_uri_warning",
                artifact_name="pdf_links",
                severity="INFO",
                status="PASS",
                message="Não há URI repetida em pdf_links.",
                details={},
            )

    def check_sha256_lineage(self, source_map: Dict[str, str]) -> None:
        text_mismatches = []
        for r in self.text_rows:
            expected = source_map.get(r["competencia"])
            if expected and r["source_sha256"] != expected:
                text_mismatches.append(r["competencia"])

        link_mismatches = []
        for r in self.link_rows:
            expected = source_map.get(r["competencia"])
            if expected and r["source_sha256"] != expected:
                link_mismatches.append(r["competencia"])

        if text_mismatches or link_mismatches:
            self._add(
                check_name="source_sha256_lineage",
                artifact_name="raw",
                severity="CRITICAL",
                status="FAIL",
                message="Há divergência de source_sha256 entre camadas RAW.",
                details={
                    "text_mismatches": sorted(set(text_mismatches)),
                    "link_mismatches": sorted(set(link_mismatches)),
                },
            )
        else:
            self._add(
                check_name="source_sha256_lineage",
                artifact_name="raw",
                severity="INFO",
                status="PASS",
                message="source_sha256 consistente entre source_inventory, pdf_text e pdf_links.",
                details={},
            )

    def evaluate(self) -> Dict[str, object]:
        self.load_inputs()

        self.check_latest_single_run_id()
        source_map = self.check_source_inventory()
        self.check_pdf_text(source_map)
        self.check_pdf_links(source_map)
        self.check_sha256_lineage(source_map)

        status_counts = Counter(r.status for r in self.records)
        severity_counts = Counter(r.severity for r in self.records)

        has_fail = status_counts.get("FAIL", 0) > 0
        has_warn = status_counts.get("WARN", 0) > 0

        approved = not has_fail and not (self.fail_on_warning and has_warn)

        return {
            "approved": approved,
            "status": "SUCCESS" if approved else "FAILED",
            "status_counts": dict(status_counts),
            "severity_counts": dict(severity_counts),
            "record_count": len(self.records),
        }

    def write_csv(self, rows: List[Dict[str, object]], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = list(RawConsistencyRecord.__dataclass_fields__.keys())

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return path

    def write_json(self, payload: Dict[str, object], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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

    def publish_existing_latest_registry(self, manifest_path: Optional[Path]) -> None:
        source_run = self._single_run_id(self.source_rows) or "unknown"
        text_run = self._single_run_id(self.text_rows) or "unknown"
        links_run = self._single_run_id(self.link_rows) or "empty"

        self.registry.publish(
            artifact_name="source_inventory",
            run_id=source_run,
            path=self.source_inventory_path,
            record_count=len(self.source_rows),
            status="SUCCESS",
            module="floriano.raw.source_inventory",
            manifest_path=str(manifest_path) if manifest_path else None,
        )

        self.registry.publish(
            artifact_name="pdf_text",
            run_id=text_run,
            path=self.pdf_text_path,
            record_count=len(self.text_rows),
            status="SUCCESS",
            module="floriano.raw.pdf_text_extractor",
            manifest_path=str(manifest_path) if manifest_path else None,
        )

        self.registry.publish(
            artifact_name="pdf_links",
            run_id=links_run,
            path=self.pdf_links_path,
            record_count=len(self.link_rows),
            status="SUCCESS",
            module="floriano.raw.pdf_link_extractor",
            manifest_path=str(manifest_path) if manifest_path else None,
        )

    def write_output(self, summary: Dict[str, object]) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        rows = [asdict(record) for record in self.records]

        run_dir = self.output_dir / f"run_id={self.logger.run_id}"
        csv_path = run_dir / "raw_artifact_consistency.csv"
        parquet_path = run_dir / "raw_artifact_consistency.parquet"
        summary_path = run_dir / "raw_artifact_consistency_summary.json"

        latest_csv_path = self.output_dir / "raw_artifact_consistency_latest.csv"
        latest_summary_path = self.output_dir / "raw_artifact_consistency_summary_latest.json"

        self.write_csv(rows, csv_path)
        self.write_csv(rows, latest_csv_path)
        parquet_written = self.write_parquet(rows, parquet_path)
        self.write_json(summary, summary_path)
        self.write_json(summary, latest_summary_path)

        manifest_path = self.manifest_manager.write_manifest(
            run_id=self.logger.run_id,
            module=self.MODULE,
            status=str(summary["status"]),
            input_artifacts=[
                {
                    "artifact_name": "source_inventory",
                    "path": str(self.source_inventory_path),
                    "hash": sha256_file(self.source_inventory_path),
                    "record_count": len(self.source_rows),
                },
                {
                    "artifact_name": "pdf_text",
                    "path": str(self.pdf_text_path),
                    "hash": sha256_file(self.pdf_text_path),
                    "record_count": len(self.text_rows),
                },
                {
                    "artifact_name": "pdf_links",
                    "path": str(self.pdf_links_path),
                    "hash": sha256_file(self.pdf_links_path),
                    "record_count": len(self.link_rows),
                },
            ],
            output_artifacts=[
                {
                    "artifact_name": "raw_artifact_consistency",
                    "path": str(csv_path),
                    "hash": sha256_file(csv_path),
                    "record_count": len(rows),
                },
                {
                    "artifact_name": "raw_artifact_consistency_summary",
                    "path": str(summary_path),
                    "hash": sha256_file(summary_path),
                    "record_count": 1,
                },
            ],
            record_counts={
                "source_inventory": len(self.source_rows),
                "pdf_text": len(self.text_rows),
                "pdf_links": len(self.link_rows),
                "raw_artifact_consistency": len(rows),
            },
            extra=summary,
        )

        if summary["status"] == "SUCCESS":
            self.publish_existing_latest_registry(manifest_path)

            self.registry.publish(
                artifact_name="raw_artifact_consistency",
                run_id=self.logger.run_id,
                path=latest_csv_path,
                record_count=len(rows),
                status="SUCCESS",
                module=self.MODULE,
                manifest_path=str(manifest_path),
            )

        self.logger.success(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="write_output",
            operation="write_raw_artifact_consistency",
            message="Relatório de consistência RAW gravado.",
            records_out=len(rows),
            duration_ms=timer.elapsed_ms(),
            source_file=str(csv_path),
        )

        return {
            "csv_path": str(csv_path),
            "latest_csv_path": str(latest_csv_path),
            "parquet_path": str(parquet_written) if parquet_written else None,
            "summary_path": str(summary_path),
            "latest_summary_path": str(latest_summary_path),
            "manifest_path": str(manifest_path),
        }

    def run(self) -> Dict[str, Optional[str]]:
        timer = ExecutionTimer()

        self.logger.started(
            layer="raw",
            module=self.MODULE,
            class_name=self.CLASS_NAME,
            method_name="run",
            operation="raw_artifact_consistency_pipeline",
            message="Gate de consistência dos artefatos RAW iniciado.",
        )

        try:
            summary = self.evaluate()
            outputs = self.write_output(summary)

            if not summary["approved"]:
                self.logger.failed(
                    layer="raw",
                    module=self.MODULE,
                    class_name=self.CLASS_NAME,
                    method_name="run",
                    operation="raw_artifact_consistency_pipeline",
                    message="Gate de consistência RAW reprovado.",
                    duration_ms=timer.elapsed_ms(),
                )
                self.logger.close(require_parquet=False)
                raise ValueError("Gate de consistência RAW reprovado.")

            self.logger.success(
                layer="raw",
                module=self.MODULE,
                class_name=self.CLASS_NAME,
                method_name="run",
                operation="raw_artifact_consistency_pipeline",
                message="Gate de consistência RAW aprovado.",
                records_out=len(self.records),
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
                operation="raw_artifact_consistency_pipeline",
                message="Gate de consistência RAW falhou.",
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                duration_ms=timer.elapsed_ms(),
            )
            self.logger.close(require_parquet=False)
            raise
