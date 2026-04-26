from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional


@dataclass(frozen=True)
class CompetenceRange:
    start_month: str
    end_month: str
    months: List[str]


class CompetenceRangeResolver:
    MONTH_PATTERN = re.compile(r"^20\d{2}_(0[1-9]|1[0-2])$")

    def __init__(
        self,
        default_start_month: str = "2025_05",
        as_of_date: Optional[str | date] = None,
    ) -> None:
        self.default_start_month = self.validate_month(default_start_month)
        self.as_of_date = self._parse_as_of_date(as_of_date)

    @classmethod
    def validate_month(cls, value: str) -> str:
        if not isinstance(value, str) or not cls.MONTH_PATTERN.match(value):
            raise ValueError(
                f"Competência inválida: {value!r}. Use o formato YYYY_MM, exemplo: 2026_03."
            )
        return value

    @staticmethod
    def _parse_as_of_date(value: Optional[str | date]) -> date:
        if value is None:
            return date.today()

        if isinstance(value, date):
            return value

        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                f"as_of_date inválida: {value!r}. Use o formato YYYY-MM-DD."
            ) from exc

    @staticmethod
    def previous_month(ref: date) -> str:
        year = ref.year
        month = ref.month - 1

        if month == 0:
            year -= 1
            month = 12

        return f"{year}_{month:02d}"

    @classmethod
    def month_to_tuple(cls, value: str) -> tuple[int, int]:
        value = cls.validate_month(value)
        year, month = value.split("_")
        return int(year), int(month)

    @classmethod
    def build_month_range(cls, start_month: str, end_month: str) -> List[str]:
        start_month = cls.validate_month(start_month)
        end_month = cls.validate_month(end_month)

        sy, sm = cls.month_to_tuple(start_month)
        ey, em = cls.month_to_tuple(end_month)

        if (sy, sm) > (ey, em):
            raise ValueError(
                f"Intervalo inválido: start_month={start_month} > end_month={end_month}."
            )

        months: List[str] = []
        y, m = sy, sm

        while (y < ey) or (y == ey and m <= em):
            months.append(f"{y}_{m:02d}")
            m += 1
            if m == 13:
                y += 1
                m = 1

        return months

    def resolve(
        self,
        *,
        month: Optional[str] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> CompetenceRange:
        if month and (start_month or end_month):
            raise ValueError("Use --month sozinho ou use --start-month/--end-month.")

        if month:
            resolved_month = self.validate_month(month)
            return CompetenceRange(
                start_month=resolved_month,
                end_month=resolved_month,
                months=[resolved_month],
            )

        if not start_month and not end_month:
            resolved_start = self.default_start_month
            resolved_end = self.previous_month(self.as_of_date)

        elif start_month and not end_month:
            resolved_start = self.validate_month(start_month)
            resolved_end = resolved_start

        elif end_month and not start_month:
            resolved_end = self.validate_month(end_month)
            resolved_start = resolved_end

        else:
            resolved_start = self.validate_month(start_month)  # type: ignore[arg-type]
            resolved_end = self.validate_month(end_month)      # type: ignore[arg-type]

        months = self.build_month_range(resolved_start, resolved_end)

        return CompetenceRange(
            start_month=resolved_start,
            end_month=resolved_end,
            months=months,
        )
