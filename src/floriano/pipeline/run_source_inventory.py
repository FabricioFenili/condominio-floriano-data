from __future__ import annotations

import argparse
import json

from floriano.raw.source_inventory import SourceInventoryBuilder
from floriano.utils.execution_logger import PipelineExecutionLogger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o Gate 1: inventário e integridade dos PDFs source."
    )

    parser.add_argument("--manual-upload-dir", default="data/manual_upload")
    parser.add_argument("--output-dir", default="data/raw/source_inventory")
    parser.add_argument("--audit-dir", default="data/raw/_audit/execution_log")

    parser.add_argument("--month", default=None)
    parser.add_argument("--start-month", default=None)
    parser.add_argument("--end-month", default=None)
    parser.add_argument("--default-start-month", default="2025_05")
    parser.add_argument("--as-of-date", default=None)

    parser.add_argument("--non-strict", action="store_true")
    parser.add_argument("--require-parquet", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger = PipelineExecutionLogger(
        base_dir=args.audit_dir,
        write_parquet=True,
    )

    builder = SourceInventoryBuilder(
        manual_upload_dir=args.manual_upload_dir,
        output_dir=args.output_dir,
        month=args.month,
        start_month=args.start_month,
        end_month=args.end_month,
        default_start_month=args.default_start_month,
        as_of_date=args.as_of_date,
        logger=logger,
        strict=not args.non_strict,
        require_parquet=args.require_parquet,
    )

    outputs = builder.run()

    payload = {
        "resolved_start_month": builder.competence_range.start_month,
        "resolved_end_month": builder.competence_range.end_month,
        "resolved_months": builder.competence_range.months,
        "outputs": outputs,
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
