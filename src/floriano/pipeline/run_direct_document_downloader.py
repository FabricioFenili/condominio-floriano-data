from __future__ import annotations

import argparse
import json

from floriano.raw.direct_document_downloader import DirectDocumentDownloader
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa DirectDocumentDownloader para links downloadarquivo."
    )

    parser.add_argument("--pdf-links-path", default="data/raw/pdf_links/pdf_links_latest.csv")
    parser.add_argument("--landing-downloads-dir", default="data/landing/downloads")
    parser.add_argument("--output-dir", default="data/raw/document_downloads")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--base-url", default=None)
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

    downloader = DirectDocumentDownloader(
        pdf_links_path=args.pdf_links_path,
        landing_downloads_dir=args.landing_downloads_dir,
        output_dir=args.output_dir,
        logger=logger,
        timeout_seconds=args.timeout_seconds,
        base_url=args.base_url,
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
