from __future__ import annotations

import argparse
import json

from floriano.raw.expense_local_materializer import ExpenseLocalMaterializer
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materializa fisicamente documentos de despesas dentro de cada expense_id."
    )
    parser.add_argument("--expense-downloads-path", default="data/raw/expense_document_downloads/expense_document_downloads_latest.csv")
    parser.add_argument("--expense-documents-path", default="data/raw/expense_pages/expense_documents_discovered_latest.csv")
    parser.add_argument("--expense-pages-path", default="data/raw/expense_pages/expense_pages_latest.csv")
    parser.add_argument("--landing-downloads-dir", default="data/landing/downloads")
    parser.add_argument("--output-dir", default="data/raw/expense_document_downloads")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--require-parquet", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = PipelineExecutionLogger(base_dir=args.audit_dir, write_parquet=True)
    materializer = ExpenseLocalMaterializer(
        expense_downloads_path=args.expense_downloads_path,
        expense_documents_path=args.expense_documents_path,
        expense_pages_path=args.expense_pages_path,
        landing_downloads_dir=args.landing_downloads_dir,
        output_dir=args.output_dir,
        logger=logger,
        timeout_seconds=args.timeout_seconds,
        require_parquet=args.require_parquet,
    )
    outputs = materializer.run()
    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
