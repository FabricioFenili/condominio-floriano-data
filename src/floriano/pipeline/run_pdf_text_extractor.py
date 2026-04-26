from __future__ import annotations

import argparse
import json

from floriano.raw.pdf_text_extractor import PdfTextExtractor
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa RAW PDF Text Extraction."
    )

    parser.add_argument(
        "--source-inventory-path",
        default="data/raw/source_inventory/source_inventory_latest.csv",
    )
    parser.add_argument("--output-dir", default="data/raw/pdf_text")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")
    parser.add_argument("--non-strict", action="store_true")
    parser.add_argument("--require-parquet", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger = PipelineExecutionLogger(
        base_dir=args.audit_dir,
        write_parquet=True,
    )

    extractor = PdfTextExtractor(
        source_inventory_path=args.source_inventory_path,
        output_dir=args.output_dir,
        logger=logger,
        strict=not args.non_strict,
        require_parquet=args.require_parquet,
    )

    outputs = extractor.run()

    print(json.dumps(outputs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
