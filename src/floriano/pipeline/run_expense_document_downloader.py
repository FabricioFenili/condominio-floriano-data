from __future__ import annotations

import argparse
import json

from floriano.raw.expense_document_downloader import ExpenseDocumentDownloader
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa downloader RAW dos documentos derivados das páginas de despesas."
    )

    parser.add_argument("--expense-documents-path", default="data/raw/expense_pages/expense_documents_discovered_latest.csv")
    parser.add_argument("--landing-downloads-dir", default="data/landing/downloads")
    parser.add_argument("--output-dir", default="data/raw/expense_document_downloads")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--no-skip-existing", action="store_true")
    parser.add_argument("--require-parquet", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger = PipelineExecutionLogger(
        base_dir=args.audit_dir,
        write_parquet=True,
    )

    downloader = ExpenseDocumentDownloader(
        expense_documents_path=args.expense_documents_path,
        landing_downloads_dir=args.landing_downloads_dir,
        output_dir=args.output_dir,
        logger=logger,
        timeout_seconds=args.timeout_seconds,
        skip_existing=not args.no_skip_existing,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        limit=args.limit,
        require_parquet=args.require_parquet,
    )

    outputs = downloader.run()
    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
