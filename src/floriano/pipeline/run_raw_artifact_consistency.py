from __future__ import annotations

import argparse
import json

from floriano.raw.raw_artifact_consistency import RawArtifactConsistencyChecker
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o gate de consistência dos artefatos RAW."
    )

    parser.add_argument("--source-inventory-path", default="data/raw/source_inventory/source_inventory_latest.csv")
    parser.add_argument("--pdf-text-path", default="data/raw/pdf_text/pdf_text_latest.csv")
    parser.add_argument("--pdf-links-path", default="data/raw/pdf_links/pdf_links_latest.csv")
    parser.add_argument("--output-dir", default="data/raw/quality")
    parser.add_argument("--latest-registry-dir", default="data/raw/_latest")
    parser.add_argument("--manifest-dir", default="data/raw/_manifests")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--require-parquet", action="store_true")
    parser.add_argument("--fail-on-warning", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger = PipelineExecutionLogger(
        base_dir=args.audit_dir,
        write_parquet=True,
    )

    checker = RawArtifactConsistencyChecker(
        source_inventory_path=args.source_inventory_path,
        pdf_text_path=args.pdf_text_path,
        pdf_links_path=args.pdf_links_path,
        output_dir=args.output_dir,
        latest_registry_dir=args.latest_registry_dir,
        manifest_dir=args.manifest_dir,
        logger=logger,
        require_parquet=args.require_parquet,
        fail_on_warning=args.fail_on_warning,
    )

    outputs = checker.run()

    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
