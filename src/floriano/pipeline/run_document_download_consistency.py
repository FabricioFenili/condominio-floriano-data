from __future__ import annotations

import argparse
import json

from floriano.raw.document_download_consistency import DocumentDownloadConsistencyChecker
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o gate de consistência dos documentos baixados."
    )

    parser.add_argument("--document-downloads-path", default="data/raw/document_downloads/document_downloads_latest.csv")
    parser.add_argument("--pdf-links-path", default="data/raw/pdf_links/pdf_links_latest.csv")
    parser.add_argument("--output-dir", default="data/raw/quality")
    parser.add_argument("--latest-registry-dir", default="data/raw/_latest")
    parser.add_argument("--manifest-dir", default="data/raw/_manifests")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--require-parquet", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger = PipelineExecutionLogger(
        base_dir=args.audit_dir,
        write_parquet=True,
    )

    checker = DocumentDownloadConsistencyChecker(
        document_downloads_path=args.document_downloads_path,
        pdf_links_path=args.pdf_links_path,
        output_dir=args.output_dir,
        latest_registry_dir=args.latest_registry_dir,
        manifest_dir=args.manifest_dir,
        logger=logger,
        require_parquet=args.require_parquet,
    )

    outputs = checker.run()

    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
