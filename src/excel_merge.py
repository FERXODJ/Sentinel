from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from openpyxl.reader.excel import load_workbook


def _norm_header(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s


def _digits(s: str) -> str:
    matches = re.findall(r"\d+", (s or ""))
    if not matches:
        return ""
    # Usa el grupo de dígitos más largo para evitar casos tipo "1.23E+05" donde el primer match sería "1".
    return max(matches, key=len)


_SCI_RE = re.compile(r"^\s*[-+]?\d+(?:\.\d+)?[eE][-+]?\d+\s*$")


def _id_key(value) -> str:
    """Normaliza IDs de Excel/strings a una llave comparable.

    - Acepta int/float/str.
    - Maneja notación científica (ej: '1.35921E+05').
    - Extrae dígitos (grupo más largo).
    - Quita ceros a la izquierda.
    """
    if value is None:
        return ""

    if isinstance(value, bool):
        return ""

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        try:
            return str(int(value))
        except Exception:
            pass

    s = str(value).strip()
    if not s:
        return ""

    if _SCI_RE.match(s):
        try:
            num = Decimal(s)
            s = str(int(num))
        except (InvalidOperation, ValueError):
            pass

    d = _digits(s)
    if not d:
        return ""
    d2 = d.lstrip("0")
    return d2 if d2 else "0"


@dataclass(frozen=True)
class SheetColumns:
    header_to_col: Dict[str, int]  # normalized header -> 1-based col index


def _get_sheet_columns(ws) -> SheetColumns:
    header_to_col: Dict[str, int] = {}
    for col_idx in range(1, ws.max_column + 1):
        val = ws.cell(row=1, column=col_idx).value
        if val is None:
            continue
        key = _norm_header(str(val))
        if key:
            header_to_col[key] = col_idx
    return SheetColumns(header_to_col=header_to_col)


def _require(cols: SheetColumns, name: str) -> int:
    key = _norm_header(name)
    if key not in cols.header_to_col:
        raise KeyError(f"No se encontró la columna '{name}' en el Excel.")
    return cols.header_to_col[key]


def merge_tickets_customers(
    excel_path: str | Path,
    tickets_sheet: str = "Datos de Tickets",
    customers_sheet: str = "Datos Clientes",
    output_sheet: str = "Datos Completos",
    not_found_sheet: str = "Datos no Encontrados",
    summary_sheet: str = "Resumen Merge",
) -> Tuple[int, int, int]:
    """Crea/rehace una hoja 'Datos Completos' haciendo join por ID.

    Join:
      - Tickets['ID Cliente'] (solo dígitos)
      - Clientes['ID`, solo dígitos]

    Escribe:
      - Columnas de Tickets: ID, Tema, Customer/Lead, Prioridad, Estado, Group, Tipo, Asignado a, Watching, ID Cliente
      - Columnas de Clientes: Socio, Residencia/Urbanización

    Returns: (tickets_rows_total, rows_joined, rows_not_found)
    """

    excel_path = Path(excel_path)
    if not excel_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {excel_path}")

    wb = load_workbook(excel_path)
    if tickets_sheet not in wb.sheetnames:
        raise KeyError(f"No existe la hoja '{tickets_sheet}'")
    if customers_sheet not in wb.sheetnames:
        raise KeyError(f"No existe la hoja '{customers_sheet}'")

    ws_t = wb[tickets_sheet]
    ws_c = wb[customers_sheet]

    t_cols = _get_sheet_columns(ws_t)
    c_cols = _get_sheet_columns(ws_c)

    # Tickets
    t_id = _require(t_cols, "ID")
    t_tema = _require(t_cols, "Tema")
    t_customer = _require(t_cols, "Customer/Lead")
    t_prioridad = _require(t_cols, "Prioridad")
    t_estado = _require(t_cols, "Estado")
    t_group = _require(t_cols, "Group")
    t_tipo = _require(t_cols, "Tipo")
    t_asignado = _require(t_cols, "Asignado a")
    t_watching = _require(t_cols, "Watching")
    t_id_cliente = _require(t_cols, "ID Cliente")

    # Clientes
    c_id = _require(c_cols, "ID")
    c_socio = _require(c_cols, "Socio")
    c_res = _require(c_cols, "Residencia/Urbanización")

    # Build map: customer_id -> (socio, residencia)
    customer_map: Dict[str, Tuple[str, str]] = {}
    customer_row_by_id: Dict[str, int] = {}
    for r in range(2, ws_c.max_row + 1):
        cid = _id_key(ws_c.cell(row=r, column=c_id).value)
        if not cid:
            continue
        socio = str(ws_c.cell(row=r, column=c_socio).value or "").strip()
        res = str(ws_c.cell(row=r, column=c_res).value or "").strip()
        customer_map[cid] = (socio, res)
        customer_row_by_id[cid] = r

    # Recreate output sheet
    if output_sheet in wb.sheetnames:
        wb.remove(wb[output_sheet])
    ws_o = wb.create_sheet(title=output_sheet)

    if not_found_sheet in wb.sheetnames:
        wb.remove(wb[not_found_sheet])
    ws_nf = wb.create_sheet(title=not_found_sheet)

    # Sheet de resumen/diagnóstico
    if summary_sheet in wb.sheetnames:
        wb.remove(wb[summary_sheet])
    ws_s = wb.create_sheet(title=summary_sheet)

    out_headers = [
        "ID",
        "Tema",
        "Customer/Lead",
        "Prioridad",
        "Estado",
        "Group",
        "Tipo",
        "Asignado a",
        "Watching",
        "ID Cliente",
        "Socio",
        "Residencia/Urbanización",
    ]
    ws_o.append(out_headers)

    nf_headers = [
        "ID",
        "Tema",
        "Customer/Lead",
        "Estado",
        "Tipo",
        "Asignado a",
        "ID Cliente",
    ]
    ws_nf.append(nf_headers)

    tickets_total = 0
    joined = 0
    not_found = 0
    not_found_counts: Dict[str, int] = {}

    # Stats básicos previos (ayuda a detectar exportación incompleta/filtrada)
    ws_s.append(["Métrica", "Valor"])
    ws_s.append(["Tickets (filas en hoja)", ws_t.max_row - 1])
    ws_s.append(["Clientes (filas en hoja)", ws_c.max_row - 1])
    ws_s.append(["Clientes (IDs únicos)", len(customer_map)])

    def _safe_int(x: str) -> int | None:
        try:
            return int(x)
        except Exception:
            return None

    c_ints = [v for v in (_safe_int(k) for k in customer_map.keys()) if v is not None]
    if c_ints:
        ws_s.append(["Clientes ID min", min(c_ints)])
        ws_s.append(["Clientes ID max", max(c_ints)])

    for r in range(2, ws_t.max_row + 1):
        tickets_total += 1
        tid_cliente = _id_key(ws_t.cell(row=r, column=t_id_cliente).value)
        if not tid_cliente:
            not_found += 1
            not_found_counts[""] = not_found_counts.get("", 0) + 1
            ws_nf.append(
                [
                    ws_t.cell(row=r, column=t_id).value,
                    ws_t.cell(row=r, column=t_tema).value,
                    ws_t.cell(row=r, column=t_customer).value,
                    ws_t.cell(row=r, column=t_estado).value,
                    ws_t.cell(row=r, column=t_tipo).value,
                    ws_t.cell(row=r, column=t_asignado).value,
                    "",
                ]
            )
            continue
        extra = customer_map.get(tid_cliente)
        if not extra:
            not_found += 1
            not_found_counts[tid_cliente] = not_found_counts.get(tid_cliente, 0) + 1
            ws_nf.append(
                [
                    ws_t.cell(row=r, column=t_id).value,
                    ws_t.cell(row=r, column=t_tema).value,
                    ws_t.cell(row=r, column=t_customer).value,
                    ws_t.cell(row=r, column=t_estado).value,
                    ws_t.cell(row=r, column=t_tipo).value,
                    ws_t.cell(row=r, column=t_asignado).value,
                    tid_cliente,
                ]
            )
            continue

        socio, res = extra
        row = [
            ws_t.cell(row=r, column=t_id).value,
            ws_t.cell(row=r, column=t_tema).value,
            ws_t.cell(row=r, column=t_customer).value,
            ws_t.cell(row=r, column=t_prioridad).value,
            ws_t.cell(row=r, column=t_estado).value,
            ws_t.cell(row=r, column=t_group).value,
            ws_t.cell(row=r, column=t_tipo).value,
            ws_t.cell(row=r, column=t_asignado).value,
            ws_t.cell(row=r, column=t_watching).value,
            tid_cliente,
            socio,
            res,
        ]
        ws_o.append(row)
        joined += 1

    # Completa resumen
    ws_s.append(["Tickets total (procesados)", tickets_total])
    ws_s.append(["Coincidencias (join)", joined])
    ws_s.append(["No encontrados", not_found])

    # Lista de IDs no encontrados (top) con conteo
    ws_s.append([""])  # separador
    ws_s.append(["Top IDs no encontrados", "Conteo", "Ejemplo fila ticket", "Fila cliente (si existe)"])

    # Solo IDs con valor (evita la llave vacía)
    items = [(k, v) for k, v in not_found_counts.items() if k]
    items.sort(key=lambda kv: kv[1], reverse=True)
    # Limitar para no inflar el Excel
    for k, v in items[:200]:
        ws_s.append([k, v, "", customer_row_by_id.get(k, "")])

    wb.save(excel_path)
    return tickets_total, joined, not_found
