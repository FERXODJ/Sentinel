from __future__ import annotations

import os
import threading
from datetime import date
from dataclasses import dataclass
from typing import Callable, Any, Protocol

from playwright.sync_api import sync_playwright, Page

from .table_extract import extract_table_to_csv


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
        self._shutdown_event = threading.Event()

        self._page_ready = threading.Event()
        self._page: Page | None = None

    def request_extract(self, table_key: str) -> None:
        if table_key not in self._extract_events:
            self._message(f"Tabla desconocida: {table_key}")
            return
        if not self._page_ready.is_set():
            self._message("Aún no está listo el navegador. Espera unos segundos y vuelve a intentar.")
            return
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
        opened = page.locator("#opened-page")
        if opened.count() > 0:
            try:
                tag = opened.first.evaluate("el => el.tagName.toLowerCase()")
            except Exception:
                tag = None

            if tag == "iframe":
                return page.frame_locator("#opened-page")

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

                # Espera hasta que el input tenga un valor no vacío
                handle = loc.element_handle()
                if handle is None:
                    raise RuntimeError("No se pudo obtener el elemento del input Period")

                loc.page.wait_for_function(
                    "el => (el && typeof el.value === 'string' && el.value.trim().length > 0)",
                    arg=handle,
                    timeout=timeout_ms,
                )
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
                self._do_extract(page, table_key="table1")

            if self._extract_events["table2"].is_set():
                self._extract_events["table2"].clear()
                self._do_extract(page, table_key="table2")

            page.wait_for_timeout(250)

    def _do_extract(self, page: Page, table_key: str) -> None:
        table_cfg = self._config.tables.get(table_key)
        if not table_cfg:
            self._message(f"No hay config para {table_key} en config.json")
            return

        scope = self._get_scope(page)

        try:
            # Pasos previos configurables (para navegación en la UI antes de extraer)
            steps: list[Any] = list(table_cfg.get("steps", []))

            if table_key == "table1" and not steps:
                # Defaults según los selectores que nos diste (Tickets -> List)
                steps = [
                    # Tickets
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(4) > div > a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[4]/div/a",
                    # List
                    "css=body > div.splynx-wrapper > div.main > nav > div > div > div.sidebar-menu > div > div.menu-list > div:nth-child(4) > div > div > div:nth-child(2) > div > a||xpath=/html/body/div[2]/div[3]/nav/div/div/div[2]/div/div[1]/div[4]/div/div/div[2]/div/a",
                    # Quick access dropdown
                    "css=#select2-admin_support_tickets_opened_filter_quick_access-container||xpath=//*[@id='select2-admin_support_tickets_opened_filter_quick_access-container']",
                    # All tickets option (id suele cambiar; preferimos texto/xpath)
                    "css=li[id^='select2-admin_support_tickets_opened_filter_quick_access-result-']:has-text('All tickets')||xpath=//li[starts-with(@id,'select2-admin_support_tickets_opened_filter_quick_access-result-')][contains(.,'All tickets')]||text=All tickets||xpath=/html/body/span/span/span[2]/ul/li[4]",
                    # Status dropdown
                    "css=#select2-admin_support_tickets_opened_filter_status-container||xpath=//*[@id='select2-admin_support_tickets_opened_filter_status-container']",
                    # Fill search with 'All'
                    {
                        "action": "fill",
                        "selector": "css=body > span > span > span.select2-search.select2-search--dropdown > input||xpath=/html/body/span/span/span[1]/input||xpath=//*[@id='opened-page']/body/span/span/span[1]/input",
                        "text": "All",
                    },
                    # Click All result
                    "css=#select2-admin_support_tickets_opened_filter_status-results li.select2-results__option--highlighted||css=#select2-admin_support_tickets_opened_filter_status-results li.select2-results__option:has-text('All')||xpath=//*[@id='select2-admin_support_tickets_opened_filter_status-results']/li[32]||xpath=/html/body/span/span/span[2]/ul/li[32]",

                    # Filter button
                    "css=#content > div > div.splynx-top-nav > div.filters-nav > div > div:nth-child(6) > button||xpath=//*[@id='content']/div/div[1]/div[2]/div/div[6]/button||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[1]/div[2]/div/div[6]/button||text=Filter",
                    # Group dropdown
                    "css=#select2-admin_support_tickets_opened_search_widget_group_id-container||xpath=//*[@id='select2-admin_support_tickets_opened_search_widget_group_id-container']",
                    # Buscar "Centro de Operaciones" en Select2
                    {
                        "action": "fill",
                        "selector": "css=body > span > span > span.select2-search.select2-search--dropdown > input||xpath=/html/body/span/span/span[1]/input||xpath=//*[@id='opened-page']/body/span/span/span[1]/input",
                        "text": "Centro de Operaciones",
                    },
                    # Seleccionar la opción
                    "css=li[id^='select2-admin_support_tickets_opened_search_widget_group_id-result-']:has-text('Centro de Operaciones')||xpath=//li[starts-with(@id,'select2-admin_support_tickets_opened_search_widget_group_id-result-')][contains(.,'Centro de Operaciones')]||xpath=/html/body/span/span/span[2]/ul/li[5]",

                    # Period: llenar rango de fechas (desde inicio de mes hasta hoy)
                    {
                        "action": "wait_nonempty",
                        "selector": "css=#admin_support_tickets_opened_search_widget_created_at||xpath=//*[@id='admin_support_tickets_opened_search_widget_created_at']||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/form/div/div[2]/div/div/input[1]",
                        "timeout_ms": 300000
                    },
                    # Aplicar filtros
                    "css=button.advanced-filter-apply-button:has-text('Aplicar')||css=button.advanced-filter-apply-button:has-text('Apply')||css=#admin_support_tickets_opened_search_block button.advanced-filter-apply-button||css=#admin_support_tickets_opened_search_block > div > div > div > button.btn.btn-primary.ms-4.advanced-filter-apply-button||xpath=//*[@id='admin_support_tickets_opened_search_block']/div/div/div/button[2]||xpath=/html/body/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div/div/button[2]||text=Aplicar||text=Apply",
                ]

            # Si hay steps, ejecutarlos en orden. Permitimos fallbacks por paso con separador "||".
            if steps:
                self._message(f"Ejecutando navegación previa para {table_key}...")
                for step in steps:
                    self._run_step(page, scope, step)
                    page.wait_for_timeout(600)

            # Por ahora, "table1" SOLO hace clics (sin extraer nada).
            if table_key == "table1":
                self._message("Listo: navegación completada (Tickets → List). Aún no se extrae ninguna tabla.")
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
