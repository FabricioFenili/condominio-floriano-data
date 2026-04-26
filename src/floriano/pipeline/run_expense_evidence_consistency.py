from __future__ import annotations

import argparse
import json

from floriano.raw.expense_evidence_consistency import ExpenseEvidenceConsistencyChecker
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o gate de consistência das evidências RAW de despesas."
    )

    parser.add_argument("--expense-pages-path", default="data/raw/expense_pages/expense_pages_latest.csv")
    parser.add_argument("--expense-documents-path", default="data/raw/expense_pages/expense_documents_discovered_latest.csv")
    parser.add_argument("--expense-downloads-path", default="data/raw/expense_document_downloads/expense_document_downloads_latest.csv")
    parser.add_argument("--output-dir", default="data/raw/quality")
    parser.add_argument("--latest-registry-dir", default="data/raw/_latest")
    parser.add_argument("--manifest-dir", default="data/raw/_manifests")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--require-parquet", action="store_true")
    parser.add_argument("--allow-empty-downloads", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger = PipelineExecutionLogger(
        base_dir=args.audit_dir,
        write_parquet=True,
    )

    checker = ExpenseEvidenceConsistencyChecker(
        expense_pages_path=args.expense_pages_path,
        expense_documents_path=args.expense_documents_path,
        expense_downloads_path=args.expense_downloads_path,
        output_dir=args.output_dir,
        latest_registry_dir=args.latest_registry_dir,
        manifest_dir=args.manifest_dir,
        logger=logger,
        require_parquet=args.require_parquet,
        allow_empty_downloads=args.allow_empty_downloads,
    )

    outputs = checker.run()
    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
