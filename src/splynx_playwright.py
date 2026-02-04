from __future__ import annotations

import os
import threading
import time
from datetime import date
from dataclasses import dataclass
from typing import Callable, Any, Protocol

from playwright.sync_api import sync_playwright, Page

from .table_extract import extract_table_to_csv, extract_tickets_to_excel, extract_customers_to_excel


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
        for frame_id in ("opened-page", "list-page"):
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

            page.wait_for_timeout(250)

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
