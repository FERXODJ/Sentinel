from __future__ import annotations

import csv
from typing import List

from typing import Protocol


class LocatorScope(Protocol):
    def locator(self, selector: str):  # playwright's Locator
        ...


def extract_table_to_csv(scope: LocatorScope, selector: str, output_csv: str) -> None:
    table = scope.locator(selector).first
    table.wait_for(state="visible")

    row_loc = table.locator("tr")
    row_count = row_loc.count()

    rows: List[List[str]] = []
    max_cols = 0

    for i in range(row_count):
        row = row_loc.nth(i)
        cell_loc = row.locator("th, td")
        cell_count = cell_loc.count()
        cells = []
        for j in range(cell_count):
            txt = cell_loc.nth(j).inner_text().strip()
            txt = " ".join(txt.split())
            cells.append(txt)
        max_cols = max(max_cols, len(cells))
        if cells:
            rows.append(cells)

    # Normaliza filas para que todas tengan el mismo número de columnas
    for r in rows:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))

    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerows(rows)
