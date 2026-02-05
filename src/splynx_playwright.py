from __future__ import annotations

import os
import re
import threading
import time
from datetime import date
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable, Any, Protocol
import unicodedata

from playwright.sync_api import sync_playwright, Page

from openpyxl import Workbook
from openpyxl.reader.excel import load_workbook

from .table_extract import extract_table_to_csv, extract_tickets_to_excel, extract_customers_to_excel
from .excel_merge import merge_tickets_customers


MessageSink = Callable[[str], Any]


class LocatorScope(Protocol):
    def locator(self, selector: str):  # playwright's Locator
        ...


@dataclass(frozen=True)
class SplynxConfig:
    login_url: str
    selectors: dict[str, str]
    tables: dict[str, dict[str, Any]]
    browser: dict[str, Any]


class SplynxSession:
    def __init__(self, config: SplynxConfig, message_sink: MessageSink) -> None:
        self._config = config
        self._message = message_sink

        self._extract_events: dict[str, threading.Event] = {
            "table1": threading.Event(),
            "table2": threading.Event(),
        }
        self._extract_modes: dict[str, str] = {
            "table1": "auto",
            "table2": "auto",
        }
        self._shutdown_event = threading.Event()

        self._enrich_event = threading.Event()
        self._enrich_excel_path: str | None = None
        self._enrich_running = False

        self._collect_dates_event = threading.Event()
        self._collect_ticket_id: str | None = None

        self._page_ready = threading.Event()
        self._page: Page | None = None

    def request_extract(self, table_key: str, mode: str = "auto") -> None:
        if table_key not in self._extract_events:
            self._message(f"Tabla desconocida: {table_key}")
            return
        if not self._page_ready.is_set():
            self._message("Aún no está listo el navegador. Espera unos segundos y vuelve a intentar.")
            return
        self._extract_modes[table_key] = str(mode or "auto").lower()
        self._extract_events[table_key].set()

    def shutdown(self) -> None:
        self._shutdown_event.set()
        for ev in self._extract_events.values():
            ev.set()
        self._enrich_event.set()

    def request_enrich_missing(self, *, excel_path: str) -> None:
        if not self._page_ready.is_set():
            self._message("Aún no está listo el navegador. Espera unos segundos y vuelve a intentar.")
            return
        if self._enrich_running:
            self._message("La búsqueda/enriquecimiento ya está ejecutándose.")
            return
        self._enrich_excel_path = str(excel_path)
        self._enrich_event.set()

    def request_collect_dates_nav(self, *, ticket_id: str) -> None:
        """Navega vía Fast Search al ticket (para luego extraer fechas)."""
        if not self._page_ready.is_set():
            self._message("Aún no está listo el navegador. Espera unos segundos y vuelve a intentar.")
            return

        tid = str(ticket_id or "").strip()
        if not tid:
            self._message("Fechas Esc/Cie: no se recibió Ticket ID.")
            return

        self._collect_ticket_id = tid
        self._collect_dates_event.set()

    def run(self, username: str, password: str) -> None:
        channel = str(self._config.browser.get("channel", "msedge"))
        headless = bool(self._config.browser.get("headless", False))

        with sync_playwright() as p:
            browser = p.chromium.launch(channel=channel, headless=headless)
            context = browser.new_context()
            page = context.new_page()
            self._page = page

            page.set_default_timeout(60_000)

            self._message("Cargando página de login...")
            page.goto(self._config.login_url, wait_until="domcontentloaded")

            user_sel = self._config.selectors.get("username", "#login")
            pass_sel = self._config.selectors.get("password", "#password")

            self._message("Colocando usuario/contraseña en el formulario...")
            page.locator(user_sel).fill(username)
            page.locator(pass_sel).fill(password)

            self._page_ready.set()
            self._message(
                "Listo: completa 2FA y presiona Login MANUALMENTE en Edge. "
                "Luego navega a la pantalla de la tabla y usa los botones de extracción."
            )

            try:
                self._event_loop(page)
            finally:
                try:
                    context.close()
                finally:
                    browser.close()

    def _get_scope(self, page: Page) -> LocatorScope:
        # Splynx suele renderizar vistas dentro de iframes con distintos IDs según la pantalla.
        for frame_id in ("opened-page", "list-page", "opened--view-page"):
            frame_loc = page.locator(f"#{frame_id}")
            if frame_loc.count() > 0:
                try:
                    tag = frame_loc.first.evaluate("el => el.tagName.toLowerCase()")
                except Exception:
                    tag = None

                if tag == "iframe":
                    return page.frame_locator(f"#{frame_id}")

        return page

    def _click_any(self, scope: LocatorScope, selectors: list[str]) -> None:
        last_exc: Exception | None = None
        for sel in selectors:
            try:
                loc = scope.locator(sel).first
                loc.wait_for(state="visible")
                loc.scroll_into_view_if_needed()
                try:
                    loc.click()
                except Exception:
                    # Algunos overlays (Select2) pueden bloquear el click normal.
                    try:
                        loc.click(force=True)
                    except Exception:
                        # Último recurso: ejecutar click vía JS.
                        loc.evaluate("el => el.click()")
                return
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc:
            raise last_exc

    def _fill_any(self, scope: LocatorScope, selectors: list[str], text: str) -> None:
        last_exc: Exception | None = None
        for sel in selectors:
            try:
                loc = scope.locator(sel).first
                loc.wait_for(state="visible")
                loc.scroll_into_view_if_needed()
                loc.fill(text)
                return
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc:
            raise last_exc

    def _press_any(self, scope: LocatorScope, selectors: list[str], key: str) -> None:
        last_exc: Exception | None = None
        for sel in selectors:
            try:
                loc = scope.locator(sel).first
                loc.wait_for(state="visible")
                loc.scroll_into_view_if_needed()
                loc.press(key)
                return
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc:
            raise last_exc

    def _wait_nonempty_any(self, scope: LocatorScope, selectors: list[str], timeout_ms: int) -> None:
        last_exc: Exception | None = None
        for sel in selectors:
            try:
                loc = scope.locator(sel).first
                loc.wait_for(state="visible")
                loc.scroll_into_view_if_needed()

                # Espera hasta que el input tenga un valor no vacío.
                # Importante: hacerlo en el contexto correcto (iframe) vía Locator.
                deadline = time.monotonic() + (timeout_ms / 1000.0)
                while True:
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"Timeout esperando valor no vacío en: {sel}")

                    try:
                        val = loc.evaluate(
                            """el => {
                                if (!el) return '';
                                const v1 = (typeof el.value === 'string') ? el.value : '';
                                const v2 = (typeof el.getAttribute === 'function') ? (el.getAttribute('value') || '') : '';
                                return String(v1 || v2 || '').trim();
                            }"""
                        )
                    except Exception:
                        val = ""

                    if isinstance(val, str) and val.strip():
                        break

                    time.sleep(0.25)

                return
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc:
            raise last_exc

    def _wait_enabled_any(self, scope: LocatorScope, selectors: list[str], timeout_ms: int) -> None:
        last_exc: Exception | None = None
        for sel in selectors:
            try:
                loc = scope.locator(sel).first
                loc.wait_for(state="visible")
                loc.scroll_into_view_if_needed()

                deadline = time.monotonic() + (timeout_ms / 1000.0)
                while True:
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"Timeout esperando botón habilitado: {sel}")

                    try:
                        if loc.is_enabled():
                            break
                    except Exception:
                        pass

                    # Fallback para elementos que no soportan is_enabled()
                    try:
                        ok = loc.evaluate(
                            """el => {
                                if (!el) return false;
                                const disabled = el.disabled === true;
                                const aria = (el.getAttribute && (el.getAttribute('aria-disabled') || '')).toLowerCase();
                                const cls = (el.getAttribute && (el.getAttribute('class') || '')).toLowerCase();
                                return !disabled && aria !== 'true' && !cls.includes('disabled');
                            }"""
                        )
                        if bool(ok):
                            break
                    except Exception:
                        pass

                    time.sleep(0.25)

                return
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc:
            raise last_exc

    def _render_text(self, text: str) -> str:
        # Macros simples para fechas:
        # - {today:%d/%m/%Y}
        # - {month_start:%d/%m/%Y}
        today = date.today()
        month_start = today.replace(day=1)

        rendered = text
        rendered = rendered.replace("{today:%d/%m/%Y}", today.strftime("%d/%m/%Y"))
        rendered = rendered.replace("{month_start:%d/%m/%Y}", month_start.strftime("%d/%m/%Y"))
        return rendered

    def _run_step(self, page: Page, scope: LocatorScope, step: Any) -> None:
        # Backward compatible:
        # - string => click
        # - {"action": "click", "selector": "..."}
        # - {"action": "fill", "selector": "...", "text": "..."}
        if isinstance(step, str):
            candidates = [s.strip() for s in step.split("||") if s.strip()]
            if not candidates:
                return
            try:
                self._click_any(scope, candidates)
                return
            except Exception:
                # Algunos dropdowns de Select2 salen fuera del iframe; probamos en la page raíz.
                if scope is not page:
                    self._click_any(page, candidates)
                else:
                    raise
            return

        if isinstance(step, dict):
            action = str(step.get("action", "click")).lower()
            selector = step.get("selector")
            if not selector:
                return

            if isinstance(selector, str):
                candidates = [s.strip() for s in selector.split("||") if s.strip()]
            else:
                candidates = [str(s).strip() for s in selector if str(s).strip()]

            if action == "fill":
                text = self._render_text(str(step.get("text", "")))
                try:
                    self._fill_any(scope, candidates, text)
                except Exception:
                    if scope is not page:
                        self._fill_any(page, candidates, text)
                    else:
                        raise
                return

            if action == "press":
                key = str(step.get("key", "Enter"))
                try:
                    self._press_any(scope, candidates, key)
                except Exception:
                    if scope is not page:
                        self._press_any(page, candidates, key)
                    else:
                        raise
                return

            if action == "wait_enabled":
                timeout_ms = int(step.get("timeout_ms", 120_000))
                try:
                    self._wait_enabled_any(scope, candidates, timeout_ms=timeout_ms)
                except Exception:
                    if scope is not page:
                        self._wait_enabled_any(page, candidates, timeout_ms=timeout_ms)
                    else:
                        raise
                return

            if action == "wait_nonempty":
                timeout_ms = int(step.get("timeout_ms", 300_000))  # 5 minutos por defecto
                try:
                    self._wait_nonempty_any(scope, candidates, timeout_ms=timeout_ms)
                except Exception:
                    if scope is not page:
                        self._wait_nonempty_any(page, candidates, timeout_ms=timeout_ms)
                    else:
                        raise
                return

            # default: click
            try:
                self._click_any(scope, candidates)
            except Exception:
                if scope is not page:
                    self._click_any(page, candidates)
                else:
                    raise
            return

    def _event_loop(self, page: Page) -> None:
        while not self._shutdown_event.is_set():
            if self._extract_events["table1"].is_set():
                self._extract_events["table1"].clear()
                self._do_extract(page, table_key="table1", mode=self._extract_modes.get("table1", "auto"))

            if self._extract_events["table2"].is_set():
                self._extract_events["table2"].clear()
                self._do_extract(page, table_key="table2", mode=self._extract_modes.get("table2", "auto"))

            if self._enrich_event.is_set():
                self._enrich_event.clear()
                self._do_enrich_missing(page)

            if self._collect_dates_event.is_set():
                self._collect_dates_event.clear()
                tid = self._collect_ticket_id or ""
                self._collect_ticket_id = None
                self._do_collect_dates_nav(page, ticket_id=tid)

            page.wait_for_timeout(250)

    def _do_collect_dates_nav(self, page: Page, *, ticket_id: str) -> None:
        tid = str(ticket_id or "").strip()
        if not tid:
            self._message("Fechas Esc/Cie: Ticket ID vacío.")
            return

        try:
            self._message(f"Fechas Esc/Cie: abriendo búsqueda y buscando ticket {tid}...")
            self._fast_search_fill(page, tid)
            page.wait_for_timeout(400)
            ok = self._fast_search_pick_ticket(page, tid)
            if not ok:
                self._message(
                    "Fechas Esc/Cie: no pude seleccionar el ticket en los resultados. "
                    "Confirma que el ID existe y que el panel de búsqueda muestra la opción de ticket."
                )
                return

            wanted_digits = self._id_key(tid)
            scope = self._get_scope(page)
            self._message("Fechas Esc/Cie: ticket seleccionado. Abriendo 'Acciones'...")

            # Paso 1: click en botón Acciones
            actions_candidates = [
                # Preferir explícitamente el dropdown por texto
                f"css=#admin_support_tickets_closed_sticky_sidebar_{wanted_digits} button:has-text('Acciones')",
                f"css=#admin_support_tickets_opened_sticky_sidebar_{wanted_digits} button:has-text('Acciones')",
                f"css=div[id$='_sticky_sidebar_{wanted_digits}'] button:has-text('Acciones')",
                f"css=#admin_support_tickets_closed_sticky_sidebar_{wanted_digits} button:has-text('Actions')",
                f"css=#admin_support_tickets_opened_sticky_sidebar_{wanted_digits} button:has-text('Actions')",
                f"css=div[id$='_sticky_sidebar_{wanted_digits}'] button:has-text('Actions')",

                # Fallback por clase/atributo de dropdown
                f"css=#admin_support_tickets_closed_sticky_sidebar_{wanted_digits} button.dropdown-toggle",
                f"css=#admin_support_tickets_opened_sticky_sidebar_{wanted_digits} button.dropdown-toggle",
                f"css=div[id$='_sticky_sidebar_{wanted_digits}'] button.dropdown-toggle",
                f"css=#admin_support_tickets_closed_sticky_sidebar_{wanted_digits} button[data-bs-toggle='dropdown']",
                f"css=#admin_support_tickets_opened_sticky_sidebar_{wanted_digits} button[data-bs-toggle='dropdown']",
                f"css=div[id$='_sticky_sidebar_{wanted_digits}'] button[data-bs-toggle='dropdown']",

                # Fallback exactos provistos (sirven para pruebas/inspección)
                f"css=#admin_support_tickets_closed_sticky_sidebar_{wanted_digits} > div > div.panel-actions.row-gap-4.column-gap-4 > div > button",
                f"xpath=//*[@id='admin_support_tickets_closed_sticky_sidebar_{wanted_digits}']/div/div[2]/div/button",
                f"css=#admin_support_tickets_opened_sticky_sidebar_{wanted_digits} > div > div.panel-actions.row-gap-4.column-gap-4 > div > button",
                f"xpath=//*[@id='admin_support_tickets_opened_sticky_sidebar_{wanted_digits}']/div/div[2]/div/button",
            ]

            # Primero intentar en el scope (iframe); si falla, probar en la página raíz.
            try:
                self._click_any(scope, actions_candidates)
            except Exception:
                self._click_any(page, actions_candidates)
            page.wait_for_timeout(250)

            # Paso 2: click en Show activities
            self._message("Fechas Esc/Cie: clic en 'Show activities'...")
            activities_candidates = [
                f"css=#admin_support_tickets_closed_view_show_hide_activities_{wanted_digits}",
                f"css=#admin_support_tickets_opened_view_show_hide_activities_{wanted_digits}",
                f"css=[id$='_view_show_hide_activities_{wanted_digits}']",
                f"xpath=//*[@id='admin_support_tickets_closed_view_show_hide_activities_{wanted_digits}']",
                f"xpath=//*[@id='admin_support_tickets_opened_view_show_hide_activities_{wanted_digits}']",
            ]

            # Igual: intentar en iframe primero y luego en page.
            try:
                self._click_any(scope, activities_candidates)
            except Exception:
                self._click_any(page, activities_candidates)
            page.wait_for_timeout(400)

            self._message("Fechas Esc/Cie: actividades visibles. Listo para el próximo paso.")
        except Exception as exc:
            self._message(f"Fechas Esc/Cie: error navegando al ticket: {exc}")

    def _norm_text(self, s: str) -> str:
        s = (s or "").strip().lower()
        try:
            s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
        except Exception:
            pass
        s = re.sub(r"\s+", " ", s)
        return s

    _SCI_RE = re.compile(r"^\s*[-+]?\d+(?:\.\d+)?[eE][-+]?\d+\s*$")

    def _id_key(self, value) -> str:
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

        if self._SCI_RE.match(s):
            try:
                num = Decimal(s)
                s = str(int(num))
            except (InvalidOperation, ValueError):
                pass

        matches = re.findall(r"\d+", s)
        if not matches:
            return ""
        d = max(matches, key=len)
        d2 = d.lstrip("0")
        return d2 if d2 else "0"

    def _open_or_create_workbook(self, path: str) -> Workbook:
        if not os.path.exists(path):
            return Workbook()
        try:
            return load_workbook(path)
        except PermissionError as exc:
            raise PermissionError(
                f"No se puede abrir '{path}'. Probablemente está abierto en Excel o bloqueado. "
                "Ciérralo y vuelve a intentar."
            ) from exc

    def _get_or_create_sheet(self, wb: Workbook, sheet_name: str):
        if sheet_name in wb.sheetnames:
            return wb[sheet_name]

        # Si el workbook recién creado solo tiene Sheet vacío, reutiliza
        if wb.sheetnames == ["Sheet"] and wb["Sheet"].max_row == 1 and wb["Sheet"].max_column == 1:
            ws = wb["Sheet"]
            ws.title = sheet_name
            return ws
        return wb.create_sheet(title=sheet_name)

    def _read_missing_ids(self, excel_path: str) -> list[str]:
        wb = load_workbook(excel_path)
        if "Datos no Encontrados" not in wb.sheetnames:
            raise KeyError("No existe la hoja 'Datos no Encontrados'. Ejecuta primero 'Comparar y agrupar datos'.")

        ws = wb["Datos no Encontrados"]
        headers = [str(ws.cell(row=1, column=c).value or "").strip() for c in range(1, ws.max_column + 1)]
        norm_to_col: dict[str, int] = {}
        for idx0, h in enumerate(headers):
            key = self._norm_text(h)
            if key and key not in norm_to_col:
                norm_to_col[key] = idx0 + 1

        def _col(name: str) -> int | None:
            return norm_to_col.get(self._norm_text(name))

        c_reporter_id = _col("Reporter ID")
        c_reporter_type = _col("Reporter type")
        c_id_cliente = _col("ID Cliente")

        if not c_reporter_id and not c_id_cliente:
            raise KeyError("En 'Datos no Encontrados' no existe 'Reporter ID' ni 'ID Cliente'.")

        ids: list[str] = []
        seen: set[str] = set()

        for r in range(2, ws.max_row + 1):
            reporter_id_val = self._id_key(ws.cell(row=r, column=c_reporter_id).value) if c_reporter_id else ""
            reporter_type_val = (
                self._norm_text(ws.cell(row=r, column=c_reporter_type).value)
                if c_reporter_type
                else ""
            )
            id_cliente_val = self._id_key(ws.cell(row=r, column=c_id_cliente).value) if c_id_cliente else ""

            join_id = ""
            if reporter_type_val == "customer" and reporter_id_val:
                join_id = reporter_id_val
            elif id_cliente_val:
                join_id = id_cliente_val
            elif reporter_id_val:
                join_id = reporter_id_val

            if join_id and join_id not in seen:
                seen.add(join_id)
                ids.append(join_id)

        return ids

    def _fast_search_open(self, page: Page) -> None:
        # Nota: este botón suele ser toggle. Por eso NO debemos clickear si el panel ya está abierto.
        if self._fast_search_is_open(page):
            return

        selectors = [
            "css=body > div.splynx-wrapper > div.splynx-header > ul > li:nth-child(2)",
            "xpath=//*[@id='dashboard-page']/body/div[2]/div[2]/ul/li[2]",
            "xpath=/html/body/div[2]/div[2]/ul/li[2]",
            "xpath=//*[@id='opened--view-page']/body/div[2]/div[2]/ul/li[2]",
        ]
        self._click_any(page, selectors)

    def _fast_search_is_open(self, page: Page) -> bool:
        selectors = [
            "css=body > div.splynx-wrapper > div.sidebar-wrapper > div > div.sidebar-content > div > div.search-wrapper > div > input",
            "css=div.search-wrapper input",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible():
                    return True
            except Exception:
                continue
        return False

    def _fast_search_clear(self, page: Page) -> None:
        selectors = [
            "css=body > div.splynx-wrapper > div.sidebar-wrapper > div > div.sidebar-content > div > div.search-wrapper > div > input",
            "xpath=//*[@id='dashboard-page']/body/div[2]/div[5]/div/div[2]/div/div[1]/div/input",
            "xpath=/html/body/div[2]/div[5]/div/div[2]/div/div[1]/div/input",
            "xpath=//*[@id='opened--view-page']/body/div[2]/div[5]/div/div[2]/div/div[1]/div/input",
            "css=div.search-wrapper input",
        ]
        try:
            self._click_any(page, selectors)
            self._fill_any(page, selectors, "")
        except Exception:
            # best-effort
            pass

    def _fast_search_fill(self, page: Page, customer_id: str) -> None:
        # Asegurar panel abierto y limpiar búsqueda previa.
        self._fast_search_open(page)
        page.wait_for_timeout(200)
        self._fast_search_clear(page)

        selectors = [
            "css=body > div.splynx-wrapper > div.sidebar-wrapper > div > div.sidebar-content > div > div.search-wrapper > div > input",
            "xpath=//*[@id='dashboard-page']/body/div[2]/div[5]/div/div[2]/div/div[1]/div/input",
            "xpath=/html/body/div[2]/div[5]/div/div[2]/div/div[1]/div/input",
            "xpath=//*[@id='opened--view-page']/body/div[2]/div[5]/div/div[2]/div/div[1]/div/input",
            "css=div.search-wrapper input",
        ]
        self._click_any(page, selectors)
        self._fill_any(page, selectors, str(customer_id))
        # En algunos builds, Enter ayuda a disparar el filtro.
        try:
            self._press_any(page, selectors, "Enter")
        except Exception:
            pass

    def _fast_search_pick_client(self, page: Page, customer_id: str) -> bool:
        wanted_digits = self._id_key(str(customer_id))
        if not wanted_digits:
            return False

        # Esperar a que el contenedor de resultados esté visible.
        try:
            page.locator("css=#fast_search_result").first.wait_for(state="visible", timeout=10_000)
        except Exception:
            return False

        # Preferir explícitamente la opción de perfil cuyo primer renglón empieza por "Cliente".
        # Esto evita seleccionar "Servicio de Internet" (que también incluye texto "Cliente").
        rows = page.locator("css=#fast_search_result > tr")

        deadline = time.monotonic() + 12.0
        while True:
            if time.monotonic() > deadline:
                return False
            try:
                if rows.count() > 0:
                    break
            except Exception:
                pass
            time.sleep(0.2)

        def _first_line(txt: str) -> str:
            t = "\n".join([ln.strip() for ln in (txt or "").splitlines() if ln.strip()])
            if not t:
                return ""
            return t.split("\n", 1)[0].strip()

        best_idx: int | None = None
        best_score = -1
        row_count = 0
        try:
            row_count = rows.count()
        except Exception:
            row_count = 0

        for i in range(min(row_count, 30)):
            try:
                txt = rows.nth(i).inner_text() or ""
            except Exception:
                continue

            compact = " ".join(txt.split())
            if wanted_digits not in compact:
                continue

            first = self._norm_text(_first_line(txt))
            all_norm = self._norm_text(compact)

            score = 0
            # match por ID
            score += 10
            # preferir que la primera línea sea "Cliente ..."
            if first.startswith("cliente"):
                score += 100
            # penalizar entradas que son de servicios/documentos/etc.
            if "servicio" in first or "invoice" in first or "recibo" in first or "pago" in first or "documento" in first:
                score -= 50
            # pequeño plus si explícitamente contiene "cliente:" en la primera línea
            if "cliente:" in first:
                score += 20
            # si el texto completo contiene "servicio de internet" también penaliza
            if "servicio de internet" in all_norm:
                score -= 25

            if score > best_score:
                best_score = score
                best_idx = i

        if best_idx is None:
            # Fallback: intenta click por selectores (menos confiable, pero mejor que fallar).
            candidates = [
                f"css=#fast_search_result > tr:has-text('Cliente:'):has-text('{wanted_digits}') a",
                f"css=#fast_search_result > tr:has-text('Cliente:'):has-text('{wanted_digits}') td",
                f"css=#fast_search_result > tr:has-text('{wanted_digits}') a",
                f"css=#fast_search_result > tr:has-text('{wanted_digits}') td",
            ]
            try:
                self._click_any(page, candidates)
                return True
            except Exception:
                return False

        try:
            rows.nth(best_idx).locator("td").first.click()
            return True
        except Exception:
            try:
                rows.nth(best_idx).click()
                return True
            except Exception:
                return False

    def _fast_search_pick_ticket(self, page: Page, ticket_id: str) -> bool:
        wanted_digits = self._id_key(str(ticket_id))
        if not wanted_digits:
            return False

        try:
            page.locator("css=#fast_search_result").first.wait_for(state="visible", timeout=10_000)
        except Exception:
            return False

        rows = page.locator("css=#fast_search_result > tr")
        deadline = time.monotonic() + 12.0
        while True:
            if time.monotonic() > deadline:
                return False
            try:
                if rows.count() > 0:
                    break
            except Exception:
                pass
            time.sleep(0.2)

        def _first_line(txt: str) -> str:
            t = "\n".join([ln.strip() for ln in (txt or "").splitlines() if ln.strip()])
            if not t:
                return ""
            return t.split("\n", 1)[0].strip()

        best_idx: int | None = None
        best_score = -1
        try:
            row_count = rows.count()
        except Exception:
            row_count = 0

        for i in range(min(row_count, 40)):
            try:
                txt = rows.nth(i).inner_text() or ""
            except Exception:
                continue

            compact = " ".join(txt.split())
            if wanted_digits not in compact:
                continue

            first = self._norm_text(_first_line(txt))
            all_norm = self._norm_text(compact)

            score = 0
            score += 10  # match por ID
            if "ticket" in all_norm:
                score += 80
            if first.startswith("ticket") or first.startswith("closed ticket") or first.startswith("open ticket"):
                score += 50

            # penalizar entradas que no son tickets
            if "cliente" in first or "cliente" in all_norm:
                score -= 30
            if "pago" in first or "invoice" in first or "recibo" in first or "documento" in first:
                score -= 40

            if score > best_score:
                best_score = score
                best_idx = i

        if best_idx is None:
            # Fallback explícito del usuario (puede variar, pero sirve para pruebas)
            candidates = [
                f"css=#fast_search_result > tr:has-text('{wanted_digits}'):has-text('ticket') td",
                f"css=#fast_search_result > tr:has-text('{wanted_digits}'):has-text('Ticket') td",
                "css=#fast_search_result > tr:nth-child(4) > td",
                "xpath=//*[@id='fast_search_result']/tr[4]/td",
                "xpath=/html/body/div[2]/div[5]/div/div[2]/div/div[2]/div/table/tr[4]/td",
            ]
            try:
                self._click_any(page, candidates)
                return True
            except Exception:
                return False

        try:
            rows.nth(best_idx).locator("td").first.click()
            return True
        except Exception:
            try:
                rows.nth(best_idx).click()
                return True
            except Exception:
                return False

    def _ensure_customer_info_tab(self, page: Page) -> None:
        """Fuerza la pestaña 'Información' del perfil para que los campos existan/estén visibles."""
        selectors = [
            "css=a:has-text('Información')",
            "css=li:has-text('Información') a",
            "text=Información",
            "css=a:has-text('Informacion')",
            "text=Informacion",
        ]

        # Best-effort: si ya está activa, click no hace daño.
        try:
            self._click_any(page, selectors)
        except Exception:
            pass

    def _wait_visible_any(self, page: Page, selectors: list[str], timeout_ms: int) -> bool:
        """Espera a que alguno de los selectores sea visible en page o en el scope (iframe)."""
        start = time.monotonic()
        while True:
            if (time.monotonic() - start) * 1000.0 > timeout_ms:
                return False

            # 1) scope (iframe) si aplica
            try:
                scope = self._get_scope(page)
                for sel in selectors:
                    try:
                        loc = scope.locator(sel).first
                        if loc.count() > 0 and loc.is_visible():
                            return True
                    except Exception:
                        continue
            except Exception:
                pass

            # 2) page raíz
            for sel in selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0 and loc.is_visible():
                        return True
                except Exception:
                    continue

            time.sleep(0.25)

    def _extract_socio_from_profile(self, page: Page) -> str:
        """Extrae el valor mostrado en el Select2 de 'Socio' dentro del perfil del cliente."""

        def _try(scope: LocatorScope) -> str:
            selectors = [
                "css=#admin_customers_view_form span[id^='select2-customers-partner_id-id-'][id$='-container']",
                "css=span[id^='select2-customers-partner_id-id-'][id$='-container']",
                "css=[id^='select2-customers-partner_id-id-'][id$='-container']",
            ]
            for sel in selectors:
                try:
                    loc = scope.locator(sel).first
                    if loc.count() == 0:
                        continue
                    loc.wait_for(state="visible", timeout=5_000)
                    txt = (loc.inner_text() or "").strip()
                    txt = " ".join(txt.split())
                    if txt:
                        return txt
                except Exception:
                    continue
            return ""

        try:
            v = _try(self._get_scope(page))
            if v:
                return v
        except Exception:
            pass
        return _try(page)

    def _extract_socio_from_profile_id(self, page: Page, customer_id: str) -> str:
        cid = self._id_key(customer_id)
        if not cid:
            return ""
        # Selector exacto para el ID actual
        exact = f"css=#select2-customers-partner_id-id-{cid}-container"
        try:
            loc = page.locator(exact).first
            if loc.count() > 0:
                loc.wait_for(state="visible", timeout=5_000)
                txt = (loc.inner_text() or "").strip()
                return " ".join(txt.split())
        except Exception:
            pass
        return self._extract_socio_from_profile(page)

    def _extract_res_urb_from_profile(self, page: Page) -> str:
        """Extrae el valor del input 'Residencia/Urbanización' dentro del perfil del cliente."""

        def _try(scope: LocatorScope) -> str:
            selectors = [
                "css=#admin_customers_view_form input[id^='customers-additional_attributes-res_urb-id-']",
                "css=input[id^='customers-additional_attributes-res_urb-id-']",
                "css=[id^='customers-additional_attributes-res_urb-id-']",
            ]
            for sel in selectors:
                try:
                    loc = scope.locator(sel).first
                    if loc.count() == 0:
                        continue
                    loc.wait_for(state="visible", timeout=5_000)
                    # input_value es lo más confiable para inputs
                    try:
                        v = loc.input_value(timeout=2_000)
                    except Exception:
                        v = (loc.get_attribute("value") or "").strip()
                    v = " ".join((v or "").split())
                    if v:
                        return v
                except Exception:
                    continue
            return ""

        try:
            v = _try(self._get_scope(page))
            if v:
                return v
        except Exception:
            pass
        return _try(page)

    def _extract_res_urb_from_profile_id(self, page: Page, customer_id: str) -> str:
        cid = self._id_key(customer_id)
        if not cid:
            return ""
        exact = f"css=#customers-additional_attributes-res_urb-id-{cid}"
        try:
            loc = page.locator(exact).first
            if loc.count() > 0:
                loc.wait_for(state="visible", timeout=5_000)
                try:
                    v = loc.input_value(timeout=2_000)
                except Exception:
                    v = (loc.get_attribute("value") or "").strip()
                return " ".join((v or "").split())
        except Exception:
            pass
        return self._extract_res_urb_from_profile(page)

    def _wait_customer_profile_loaded_for_id(self, page: Page, customer_id: str, timeout_ms: int) -> bool:
        """Espera que el perfil abierto corresponda al ID buscado.

        Evita el caso donde #admin_customers_view_form sigue visible del cliente anterior.
        """
        cid = self._id_key(customer_id)
        if not cid:
            return False

        # 1) URL (lo más fuerte cuando el routing es /customers/view?id=...)
        try:
            page.wait_for_function(
                f"() => String(window.location.href || '').includes('customers/view') && String(window.location.href || '').includes('id={cid}')",
                timeout=timeout_ms,
            )
            return True
        except Exception:
            pass

        # 2) Fallback: existencia de campos con sufijo del ID (selectores dinámicos)
        selectors = [
            f"css=#select2-customers-partner_id-id-{cid}-container",
            f"css=#customers-additional_attributes-res_urb-id-{cid}",
        ]
        return self._wait_visible_any(page, selectors, timeout_ms=timeout_ms)

    def _extract_value_by_label(self, page: Page, label_candidates: list[str]) -> str:
        # Intenta en scope (iframe) y luego en la página principal.
        def _try_in_scope(scope: LocatorScope) -> str:
            for root_sel in ("css=body", "css=html"):  # elemento ancla
                try:
                    root = scope.locator(root_sel).first
                    if root.count() == 0:
                        continue
                    val = root.evaluate(
                        r"""(el, labels) => {
                            const doc = el && el.ownerDocument ? el.ownerDocument : document;
                            const clean = (s) => String(s || '').replace(/\s+/g,' ').trim();
                            const norm = (s) => clean(s)
                                .toLowerCase()
                                .normalize('NFD')
                                .replace(/\p{Diacritic}/gu,'');
                            const wants = (labels || []).map(norm).filter(Boolean);
                            if (!wants.length) return '';

                            const nodes = Array.from(doc.querySelectorAll('th,td,dt,dd,label,div,span'));
                            for (const node of nodes) {
                                const t = norm(node.textContent || '');
                                if (!t) continue;
                                let hit = false;
                                for (const w of wants) {
                                    if (t === w || t === w + ':' || t.startsWith(w + ':') || t.includes(w)) {
                                        hit = true;
                                        break;
                                    }
                                }
                                if (!hit) continue;

                                // table row: <tr><th>Label</th><td>Value</td></tr>
                                const tr = node.closest('tr');
                                if (tr) {
                                    const tds = Array.from(tr.querySelectorAll('td'));
                                    if (tds.length) {
                                        const v = clean(tds[tds.length - 1].innerText || tds[tds.length - 1].textContent);
                                        if (v && norm(v) !== t) return v;
                                    }
                                }

                                // dl: <dt>Label</dt><dd>Value</dd>
                                if (node.tagName && node.tagName.toLowerCase() === 'dt') {
                                    const dd = node.nextElementSibling;
                                    if (dd && dd.tagName && dd.tagName.toLowerCase() === 'dd') {
                                        const v = clean(dd.innerText || dd.textContent);
                                        if (v) return v;
                                    }
                                }

                                // label for=input
                                const forId = node.getAttribute && node.getAttribute('for');
                                if (forId) {
                                    const inp = doc.getElementById(forId);
                                    if (inp) {
                                        const v = clean(inp.value || inp.textContent);
                                        if (v) return v;
                                    }
                                }

                                // next sibling
                                const sib = node.nextElementSibling;
                                if (sib) {
                                    const v = clean(sib.innerText || sib.textContent);
                                    if (v) return v;
                                }
                            }
                            return '';
                        }""",
                        label_candidates,
                    )
                    if isinstance(val, str) and val.strip():
                        return " ".join(val.split())
                except Exception:
                    continue
            return ""

        # 1) intentar en el iframe scope (si aplica)
        try:
            scope = self._get_scope(page)
            v = _try_in_scope(scope)
            if v:
                return v
        except Exception:
            pass

        # 2) fallback en la page raíz
        try:
            return _try_in_scope(page)
        except Exception:
            return ""

    def _upsert_customer_minimal(self, excel_path: str, *, customer_id: str, socio: str, residencia: str) -> None:
        wb = self._open_or_create_workbook(excel_path)
        ws = self._get_or_create_sheet(wb, "Datos Clientes")

        # asegurar headers mínimos
        if ws.max_row < 1:
            ws.append(["ID", "Socio", "Residencia/Urbanización"])
        if ws.max_row == 1 and (ws.cell(row=1, column=1).value is None):
            ws.append(["ID", "Socio", "Residencia/Urbanización"])

        headers = [str(ws.cell(row=1, column=c).value or "").strip() for c in range(1, ws.max_column + 1)]
        norm_to_col: dict[str, int] = {}
        for idx0, h in enumerate(headers):
            key = self._norm_text(h)
            if key and key not in norm_to_col:
                norm_to_col[key] = idx0 + 1

        def _ensure_col(name: str) -> int:
            key = self._norm_text(name)
            col = norm_to_col.get(key)
            if col:
                return col
            # crear al final
            col = ws.max_column + 1
            ws.cell(row=1, column=col).value = name
            norm_to_col[key] = col
            return col

        c_id = _ensure_col("ID")
        c_socio = _ensure_col("Socio")
        c_res = _ensure_col("Residencia/Urbanización")

        cid_key = self._id_key(customer_id)
        if not cid_key:
            return

        # buscar fila existente
        target_row = None
        for r in range(2, ws.max_row + 1):
            existing = self._id_key(ws.cell(row=r, column=c_id).value)
            if existing == cid_key:
                target_row = r
                break

        if target_row is None:
            target_row = ws.max_row + 1
            ws.cell(row=target_row, column=c_id).value = cid_key

        if socio:
            ws.cell(row=target_row, column=c_socio).value = socio
        if residencia:
            ws.cell(row=target_row, column=c_res).value = residencia

        wb.save(excel_path)

    def _do_enrich_missing(self, page: Page) -> None:
        excel_path = self._enrich_excel_path
        if not excel_path:
            self._message("Búsqueda/enriquecimiento: no se recibió ruta del Excel.")
            return
        if self._enrich_running:
            self._message("Búsqueda/enriquecimiento: ya está en progreso.")
            return

        self._enrich_running = True
        try:
            if not os.path.exists(excel_path):
                self._message(f"Búsqueda/enriquecimiento: no existe el archivo: {excel_path}")
                return

            ids = self._read_missing_ids(excel_path)
            if not ids:
                self._message("Búsqueda/enriquecimiento: no hay IDs para buscar.")
                return

            self._message(f"Búsqueda/enriquecimiento: {len(ids)} IDs únicos por buscar...")

            found = 0
            not_found = 0
            for idx, cid in enumerate(ids, start=1):
                if self._shutdown_event.is_set():
                    break

                try:
                    self._message(f"[{idx}/{len(ids)}] Buscando cliente ID {cid}...")
                    before_url = ""
                    try:
                        before_url = page.url
                    except Exception:
                        before_url = ""
                    self._fast_search_fill(page, cid)
                    page.wait_for_timeout(300)
                    ok = self._fast_search_pick_client(page, cid)
                    if not ok:
                        self._message(f"[{idx}/{len(ids)}] ID {cid}: no se pudo seleccionar el perfil (no encontrado o ambiguo).")
                        not_found += 1
                        continue

                    # Esperar navegación real (URL cambió o coincide con ID)
                    try:
                        if before_url:
                            page.wait_for_function(
                                "(u) => String(window.location.href || '') !== String(u || '')",
                                arg=before_url,
                                timeout=8_000,
                            )
                    except Exception:
                        pass

                    # Esperar que cargue el perfil CORRECTO para este ID.
                    ready = self._wait_customer_profile_loaded_for_id(page, cid, timeout_ms=25_000)
                    if not ready:
                        self._message(f"[{idx}/{len(ids)}] ID {cid}: timeout esperando el perfil. Saltando...")
                        not_found += 1
                        continue

                    # Asegurar la pestaña Información (algunas navegaciones abren #services u otra sección)
                    self._ensure_customer_info_tab(page)
                    page.wait_for_timeout(300)

                    # Esperar que los campos específicos del ID estén visibles (si existen)
                    self._wait_visible_any(
                        page,
                        [
                            f"css=#select2-customers-partner_id-id-{self._id_key(cid)}-container",
                            f"css=#customers-additional_attributes-res_urb-id-{self._id_key(cid)}",
                        ],
                        timeout_ms=8_000,
                    )

                    # Extraer los 2 campos cruciales (selectores que dependen del ID)
                    socio = self._extract_socio_from_profile_id(page, cid)
                    residencia = self._extract_res_urb_from_profile_id(page, cid)

                    # Fallback por texto/labels si algo cambió en el DOM
                    if not socio:
                        socio = self._extract_value_by_label(page, ["Socio", "Partner", "Socio:"])
                    if not residencia:
                        residencia = self._extract_value_by_label(
                            page,
                            [
                                "Residencia/Urbanización",
                                "Residencia",
                                "Urbanización",
                                "Urbanizacion",
                                "Dirección",
                                "Direccion",
                                "Address",
                            ],
                        )

                    if socio or residencia:
                        self._upsert_customer_minimal(
                            excel_path,
                            customer_id=cid,
                            socio=socio,
                            residencia=residencia,
                        )
                        found += 1
                        self._message(
                            f"[{idx}/{len(ids)}] ID {cid}: OK (Socio='{socio or '-'}', Residencia='{residencia or '-'}')."
                        )
                        page.wait_for_timeout(350)
                    else:
                        # No pudimos leer campos, pero el cliente abrió
                        self._message(f"[{idx}/{len(ids)}] ID {cid}: perfil abierto pero no se pudieron leer los campos.")
                        not_found += 1
                        page.wait_for_timeout(250)
                except PermissionError:
                    raise
                except Exception:
                    self._message(f"[{idx}/{len(ids)}] ID {cid}: error inesperado durante búsqueda/extracción.")
                    not_found += 1
                    page.wait_for_timeout(250)

                if idx % 10 == 0 or idx == len(ids):
                    self._message(
                        f"Búsqueda/enriquecimiento: progreso {idx}/{len(ids)}. Con datos: {found}, sin datos: {not_found}."
                    )

            self._message("Búsqueda/enriquecimiento: relanzando merge para actualizar 'Datos Completos'...")
            total, joined, nf = merge_tickets_customers(excel_path)
            self._message(f"OK: merge actualizado. Tickets: {total}, coincidencias: {joined}, no encontrados: {nf}.")
        except PermissionError:
            self._message(
                "Búsqueda/enriquecimiento: no pude abrir/guardar el Excel. "
                "Cierra 'output/Datos Splynx.xlsx' en Excel y vuelve a intentar."
            )
        except Exception as exc:
            self._message(f"Búsqueda/enriquecimiento: error: {exc}")
        finally:
            self._enrich_running = False

    def _do_extract(self, page: Page, table_key: str, mode: str = "auto") -> None:
        table_cfg = self._config.tables.get(table_key)
        if not table_cfg:
            self._message(f"No hay config para {table_key} en config.json")
            return

        scope = self._get_scope(page)

        try:
            mode = str(mode or "auto").lower()

            # Pasos previos configurables (para navegación en la UI antes de extraer)
            steps: list[Any] = list(table_cfg.get("steps", []))

            if table_key == "table1" and mode == "manual":
                # En modo manual el bot solo navega hasta Tickets -> List (y ajusta Acceso rápido).
                # Luego el usuario coloca filtros y presiona Aplicar.
                nav_steps: list[Any] = [
                    # Tickets
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(4) > div > a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[4]/div/a",
                    # List
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(4) > div > div > div:nth-child(2) > div > a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[4]/div/div/div[2]/div/a",
                    # Acceso rápido
                    "css=#select2-admin_support_tickets_opened_filter_quick_access-container||xpath=//*[@id='select2-admin_support_tickets_opened_filter_quick_access-container']",
                    "css=li[id^='select2-admin_support_tickets_opened_filter_quick_access-result-']:has-text('All tickets')||xpath=//li[starts-with(@id,'select2-admin_support_tickets_opened_filter_quick_access-result-')][contains(.,'All tickets')]||text=All tickets||xpath=/html/body/span/span/span[2]/ul/li[4]",
                ]

                self._message("Tabla 1 (manual): navegando a Tickets > List...")
                for step in nav_steps:
                    self._run_step(page, scope, step)
                    page.wait_for_timeout(600)

                # La navegación puede cambiar el iframe; recalcular scope.
                scope = self._get_scope(page)

                self._message(
                    "Tabla 1 (manual): abre 'Filter', coloca tus filtros y presiona 'Aplicar'. "
                    "La extracción empezará DESPUÉS de ese clic."
                )

                start_marker = self._tickets_first_row_marker(scope)
                self._wait_for_apply_click(scope, timeout_s=900.0)
                # Esperar la recarga tras Aplicar (si no cambia, igual continuamos tras un pequeño delay)
                self._wait_for_tickets_reload_after_apply(page, scope, start_marker=start_marker, timeout_s=180.0)
                output_xlsx = os.path.join("output", "Datos Splynx.xlsx")
                os.makedirs(os.path.dirname(output_xlsx) or ".", exist_ok=True)
                self._message("Extrayendo datos de Tickets a Excel...")
                page.wait_for_timeout(800)
                extract_tickets_to_excel(scope, output_xlsx=output_xlsx, sheet_name="Datos de Tickets")
                self._message(f"OK: exportado a {output_xlsx}")
                return

            if table_key == "table1" and not steps:
                # Defaults (Tickets -> List) + filtros nuevos
                steps = [
                    # Tickets
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(4) > div > a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[4]/div/a",
                    # List
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(4) > div > div > div:nth-child(2) > div > a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[4]/div/div/div[2]/div/a",
                    # Quick access dropdown (Acceso Rápido)
                    "css=#select2-admin_support_tickets_opened_filter_quick_access-container||xpath=//*[@id='select2-admin_support_tickets_opened_filter_quick_access-container']",
                    # All tickets option (id suele cambiar; preferimos texto/xpath)
                    "css=li[id^='select2-admin_support_tickets_opened_filter_quick_access-result-']:has-text('All tickets')||xpath=//li[starts-with(@id,'select2-admin_support_tickets_opened_filter_quick_access-result-')][contains(.,'All tickets')]||text=All tickets||xpath=/html/body/span/span/span[2]/ul/li[4]",
                    # Filter button
                    "css=#content > div > div.splynx-top-nav > div.filters-nav > div > div:nth-child(6) > button||xpath=//*[@id='content']/div/div[1]/div[2]/div/div[6]/button||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[1]/div[2]/div/div[6]/button||text=Filter",

                    # Condition
                    "css=#select2-admin_support_tickets_opened_search_widget_condition-container||xpath=//*[@id='select2-admin_support_tickets_opened_search_widget_condition-container']",
                    # All option (id dinámico)
                    "css=li[id^='select2-admin_support_tickets_opened_search_widget_condition-result-']:has-text('All')||css=li[id^='select2-admin_support_tickets_opened_search_widget_condition-result-']:has-text('Todos')||xpath=//li[starts-with(@id,'select2-admin_support_tickets_opened_search_widget_condition-result-')][contains(.,'All') or contains(.,'Todos')]||xpath=/html/body/span/span/span[2]/ul/li[3]||text=All||text=Todos",

                    # Group
                    "css=#select2-admin_support_tickets_opened_search_widget_group_id-container||xpath=//*[@id='select2-admin_support_tickets_opened_search_widget_group_id-container']",
                    # Buscar "Cualquiera"
                    {
                        "action": "fill",
                        "selector": "css=body > span > span > span.select2-search.select2-search--dropdown > input||xpath=/html/body/span/span/span[1]/input||xpath=//*[@id='opened-page']/body/span/span/span[1]/input||xpath=//*[@id='opened--view-page']/body/span/span/span[1]/input",
                        "text": "Cualquiera",
                    },
                    # Seleccionar opción "Cualquiera" (o resaltada)
                    "css=#select2-admin_support_tickets_opened_search_widget_group_id-results li.select2-results__option--highlighted||css=#select2-admin_support_tickets_opened_search_widget_group_id-results li.select2-results__option:has-text('Cualquiera')||css=#select2-admin_support_tickets_opened_search_widget_group_id-results li.select2-results__option:has-text('Any')||xpath=//*[@id='select2-admin_support_tickets_opened_search_widget_group_id-results']/li[contains(.,'Cualquiera') or contains(.,'Any')]||xpath=/html/body/span/span/span[2]/ul/li[16]",

                    # Socio
                    "css=#select2-admin_support_tickets_opened_search_widget_partner_id-container||xpath=//*[@id='select2-admin_support_tickets_opened_search_widget_partner_id-container']",
                    # Buscar "Cualquiera"
                    {
                        "action": "fill",
                        "selector": "css=body > span > span > span.select2-search.select2-search--dropdown > input||xpath=/html/body/span/span/span[1]/input||xpath=//*[@id='opened-page']/body/span/span/span[1]/input||xpath=//*[@id='opened--view-page']/body/span/span/span[1]/input",
                        "text": "Cualquiera",
                    },
                    # Seleccionar opción "Cualquiera" (o resaltada)
                    "css=#select2-admin_support_tickets_opened_search_widget_partner_id-results li.select2-results__option--highlighted||css=#select2-admin_support_tickets_opened_search_widget_partner_id-results li.select2-results__option:has-text('Cualquiera')||css=#select2-admin_support_tickets_opened_search_widget_partner_id-results li.select2-results__option:has-text('Any')||xpath=//ul[@id='select2-admin_support_tickets_opened_search_widget_partner_id-results']/li[contains(.,'Cualquiera') or contains(.,'Any')]||xpath=/html/body/span/span/span[2]/ul/li[1]",

                    # Period: llenar rango de fechas (desde inicio de mes hasta hoy)
                    {
                        "action": "wait_nonempty",
                        "selector": "css=#admin_support_tickets_opened_search_widget_created_at||xpath=//*[@id='admin_support_tickets_opened_search_widget_created_at']||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/form/div/div[2]/div/div/input[1]",
                        "timeout_ms": 300000
                    },
                    # Forzar commit/blur del input de Period
                    {
                        "action": "press",
                        "selector": "css=#admin_support_tickets_opened_search_widget_created_at||xpath=//*[@id='admin_support_tickets_opened_search_widget_created_at']||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/form/div/div[2]/div/div/input[1]",
                        "key": "Tab",
                    },
                    # Esperar a que el botón Aplicar esté habilitado
                    {
                        "action": "wait_enabled",
                        "selector": "css=#admin_support_tickets_opened_search_block > div > div > div > button.btn.btn-primary.ms-4.advanced-filter-apply-button||css=#admin_support_tickets_opened_search_block button.btn.btn-primary.advanced-filter-apply-button||css=button.advanced-filter-apply-button:has-text('Aplicar')||css=button.advanced-filter-apply-button:has-text('Apply')||xpath=//*[@id='admin_support_tickets_opened_search_block']/div/div/div/button[2]||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/div/button[2]||text=Aplicar||text=Apply",
                        "timeout_ms": 120000,
                    },
                    # Aplicar filtros
                    "css=button.advanced-filter-apply-button:has-text('Aplicar')||css=button.advanced-filter-apply-button:has-text('Apply')||css=#admin_support_tickets_opened_search_block button.advanced-filter-apply-button||css=#admin_support_tickets_opened_search_block > div > div > div > button.btn.btn-primary.ms-4.advanced-filter-apply-button||xpath=//*[@id='admin_support_tickets_opened_search_block']/div/div/div/button[2]||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/div/button[2]||text=Aplicar||text=Apply",
                ]

            if table_key == "table2" and not steps:
                # Defaults: Clientes -> Lista
                steps = [
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(2) > div > a",
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(2) > div > div > div:nth-child(2) > div > a||xpath=//*[@id='list-page']/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[2]/div/div/div[2]/div/a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[2]/div/div/div[2]/div/a||xpath=//*[@id='list-page']/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[2]/div/a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[2]/div/a",
                ]

            # Si hay steps, ejecutarlos en orden. Permitimos fallbacks por paso con separador "||".
            if steps:
                self._message(f"Ejecutando navegación previa para {table_key}...")
                for step in steps:
                    self._run_step(page, scope, step)
                    page.wait_for_timeout(600)

            if table_key == "table1":
                # Luego de completar filtros, exporta la tabla visible a Excel.
                output_xlsx = os.path.join("output", "Datos Splynx.xlsx")
                os.makedirs(os.path.dirname(output_xlsx) or ".", exist_ok=True)

                self._message("Extrayendo datos de Tickets a Excel...")
                # Pequeña espera para que el listado recargue tras Aplicar
                page.wait_for_timeout(1200)
                extract_tickets_to_excel(scope, output_xlsx=output_xlsx, sheet_name="Datos de Tickets")
                self._message(f"OK: exportado a {output_xlsx}")
                return

            if table_key == "table2":
                output_xlsx = os.path.join("output", "Datos Splynx.xlsx")
                os.makedirs(os.path.dirname(output_xlsx) or ".", exist_ok=True)

                self._message("Extrayendo datos de Clientes a Excel (solo esta página)...")
                page.wait_for_timeout(1200)
                extract_customers_to_excel(scope, output_xlsx=output_xlsx, sheet_name="Datos Clientes")
                self._message(f"OK: Clientes exportados a {output_xlsx} (hoja: Datos Clientes)")
                return

            selector = table_cfg.get("selector")
            output_csv = table_cfg.get("output_csv")

            if not selector or not output_csv:
                self._message(f"Config incompleta para {table_key}: falta selector u output_csv")
                return

            os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)

            self._message(f"Extrayendo {table_key} usando selector: {selector}")
            extract_table_to_csv(scope, selector=selector, output_csv=output_csv)
            self._message(f"OK: {table_key} exportada a {output_csv}")
        except Exception as exc:
            self._message(f"Error extrayendo {table_key}: {exc}")

    def _tickets_first_row_marker(self, scope: LocatorScope) -> str:
        table_sel = "css=#admin_support_tickets_opened_list"
        try:
            first_row = scope.locator(f"{table_sel} tbody tr").first
            if first_row.count() == 0:
                return ""
            return " ".join(first_row.inner_text().strip().split())
        except Exception:
            return ""

    def _wait_for_apply_click(self, scope: LocatorScope, timeout_s: float) -> None:
        """Espera a que el usuario haga clic en el botón Aplicar (en el contexto correcto/iframe)."""
        selectors = [
            "css=#admin_support_tickets_opened_search_block > div > div > div > button.btn.btn-primary.ms-4.advanced-filter-apply-button",
            "css=#admin_support_tickets_opened_search_block button.btn.btn-primary.advanced-filter-apply-button",
            "css=button.advanced-filter-apply-button:has-text('Aplicar')",
            "css=button.advanced-filter-apply-button:has-text('Apply')",
            "xpath=//*[@id='admin_support_tickets_opened_search_block']/div/div/div/button[2]",
            "xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/div/button[2]",
            "text=Aplicar",
            "text=Apply",
        ]

        start = time.monotonic()
        last_exc: Exception | None = None

        # Esperar a que el botón exista y esté visible (normalmente aparece al abrir Filter).
        apply_loc = None
        for sel in selectors:
            try:
                loc = scope.locator(sel).first
                loc.wait_for(state="visible", timeout=5_000)
                apply_loc = loc
                break
            except Exception as exc:
                last_exc = exc
                continue

        while apply_loc is None:
            if time.monotonic() - start > timeout_s:
                raise TimeoutError("Timeout esperando que aparezca el botón 'Aplicar'.")
            # reintentar encontrarlo
            for sel in selectors:
                try:
                    loc = scope.locator(sel).first
                    loc.wait_for(state="visible", timeout=2_000)
                    apply_loc = loc
                    break
                except Exception as exc:
                    last_exc = exc
                    continue
            if apply_loc is None:
                time.sleep(0.25)

        # Instalar listener una vez (en el frame donde vive el botón)
        try:
            apply_loc.evaluate(
                """el => {
                    try { window.__splynxApplyClicks = 0; } catch(e) {}
                    if (!el.__splynxBound) {
                        el.__splynxBound = true;
                        el.addEventListener('click', () => { window.__splynxApplyClicks = (window.__splynxApplyClicks || 0) + 1; }, true);
                    }
                }"""
            )
        except Exception:
            # Si no se puede instalar, igual intentamos detectar cambios de tabla luego.
            pass

        # Esperar el click
        while True:
            if time.monotonic() - start > timeout_s:
                raise TimeoutError("Timeout esperando clic en 'Aplicar'.")
            try:
                clicks = apply_loc.evaluate("() => window.__splynxApplyClicks || 0")
                if int(clicks) >= 1:
                    return
            except Exception:
                # si el botón desaparece/re-renderiza, re-encontrarlo
                apply_loc = None
                for sel in selectors:
                    try:
                        loc = scope.locator(sel).first
                        loc.wait_for(state="visible", timeout=1_000)
                        apply_loc = loc
                        break
                    except Exception:
                        continue
            time.sleep(0.25)

    def _wait_for_tickets_reload_after_apply(
        self,
        page: Page,
        scope: LocatorScope,
        timeout_s: float = 300.0,
        start_marker: str | None = None,
    ) -> None:
        """Espera una recarga de la tabla de tickets tras presionar Aplicar.

        Nota: algunos casos no muestran overlay 'processing' o la primera fila puede quedar igual;
        en ese caso no bloqueamos indefinidamente.
        """
        table_sel = "css=#admin_support_tickets_opened_list"

        if start_marker is None:
            start_marker = self._tickets_first_row_marker(scope)

        start = time.monotonic()
        seen_processing = False
        while True:
            if time.monotonic() - start > timeout_s:
                # Si nunca vimos processing ni cambió marker, igual dejamos seguir para no bloquear.
                return

            # Detectar overlay de processing (DataTables)
            try:
                proc = scope.locator("css=div.dataTables_processing").first
                if proc.count() > 0 and proc.is_visible():
                    seen_processing = True
                    time.sleep(0.25)
                    continue
            except Exception:
                pass

            # Si ya vimos processing, esperar a que haya filas (o estado vacío) y salir
            try:
                table = scope.locator(table_sel).first
                if table.count() > 0:
                    rows = table.locator("tbody tr")
                    if rows.count() > 0:
                        # estado vacío
                        if table.locator("tbody tr td.dataTables_empty").count() > 0:
                            return
                        # si cambió el primer row o al menos ya hay datos
                        if seen_processing:
                            return
                        if start_marker and self._tickets_first_row_marker(scope) != start_marker:
                            return
            except Exception:
                pass

            # Si el primer row cambia, asumimos recarga
            if start_marker and self._tickets_first_row_marker(scope) != start_marker:
                return

            time.sleep(0.25)
