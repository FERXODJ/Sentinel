import queue
import threading
import tkinter as tk
from tkinter import messagebox
import tkinter.scrolledtext as scrolledtext
from pathlib import Path

from .splynx_playwright import SplynxSession
from .util import load_config
from .excel_merge import merge_tickets_customers
from .excel_reorder import reorder_datos_completos_by_template


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        self.title("Splynx Scraper (Edge + Playwright)")
        self.geometry("720x520")
        self.minsize(720, 520)
        self.resizable(True, True)

        self._config = load_config()
        self._messages: "queue.Queue[str]" = queue.Queue()

        self._session: SplynxSession | None = None
        self._session_thread: threading.Thread | None = None
        self._merge_thread: threading.Thread | None = None
        self._reorder_thread: threading.Thread | None = None
        self._enrich_thread: threading.Thread | None = None

        self._build_ui()
        self._poll_messages()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        pad = 10

        frm = tk.Frame(self)
        frm.pack(fill=tk.BOTH, expand=True, padx=pad, pady=pad)

        tk.Label(frm, text="Usuario:").grid(row=0, column=0, sticky="w")
        self.username_var = tk.StringVar()
        self.username_entry = tk.Entry(frm, textvariable=self.username_var, width=50)
        self.username_entry.grid(row=1, column=0, columnspan=3, sticky="we", pady=(0, pad))

        tk.Label(frm, text="Contraseña:").grid(row=2, column=0, sticky="w")
        self.password_var = tk.StringVar()
        self.password_entry = tk.Entry(frm, textvariable=self.password_var, width=50, show="*")
        self.password_entry.grid(row=3, column=0, columnspan=3, sticky="we", pady=(0, pad))

        self.open_btn = tk.Button(frm, text="Abrir Splynx", command=self._on_open)
        self.open_btn.grid(row=4, column=0, sticky="we", pady=(0, pad))

        self.extract1_btn = tk.Button(frm, text="Extraer Tabla 1", command=self._on_extract1, state=tk.DISABLED)
        self.extract1_btn.grid(row=4, column=1, sticky="we", padx=(pad, 0), pady=(0, pad))

        self.extract2_btn = tk.Button(frm, text="Extraer Tabla 2", command=self._on_extract2, state=tk.DISABLED)
        self.extract2_btn.grid(row=4, column=2, sticky="we", padx=(pad, 0), pady=(0, pad))

        self.enrich_btn = tk.Button(
            frm,
            text="Buscar datos no encontrados en (Splynx)",
            command=self._on_enrich_missing,
            state=tk.DISABLED,
        )
        self.enrich_btn.grid(row=5, column=0, columnspan=3, sticky="we", pady=(0, pad))

        self.merge_btn = tk.Button(frm, text="Comparar y agrupar datos", command=self._on_merge, state=tk.NORMAL)
        self.merge_btn.grid(row=6, column=0, columnspan=3, sticky="we", pady=(0, pad))

        self.reorder_btn = tk.Button(
            frm,
            text="Reordenar Datos Completos (plantilla WOW)",
            command=self._on_reorder,
            state=tk.NORMAL,
        )
        self.reorder_btn.grid(row=7, column=0, columnspan=3, sticky="we", pady=(0, pad))

        self.status_var = tk.StringVar(value="Listo. Ingresa tus credenciales.")
        self.status = tk.Label(frm, textvariable=self.status_var, anchor="w", justify=tk.LEFT, wraplength=680)
        self.status.grid(row=8, column=0, columnspan=3, sticky="we")

        self.log = scrolledtext.ScrolledText(frm, height=8, wrap=tk.WORD)
        self.log.grid(row=9, column=0, columnspan=3, sticky="nsew", pady=(pad, 0))
        self.log.configure(state=tk.DISABLED)

        note = (
            "Flujo: el bot abre Edge y llena usuario/clave. "
            "Luego tú ingresas el 2FA y haces Login manualmente. "
            "Cuando estés en la pantalla correcta, presiona Extraer Tabla 1 o 2."
        )
        tk.Label(frm, text=note, fg="#444", wraplength=480, justify=tk.LEFT).grid(
            row=10, column=0, columnspan=3, sticky="we", pady=(pad, 0)
        )

        for c in range(3):
            frm.grid_columnconfigure(c, weight=1)

        frm.grid_rowconfigure(9, weight=1)

    def _send(self, msg: str) -> None:
        self._messages.put(msg)

    def _poll_messages(self) -> None:
        try:
            while True:
                msg = self._messages.get_nowait()
                self.status_var.set(msg)
                try:
                    self.log.configure(state=tk.NORMAL)
                    self.log.insert(tk.END, msg + "\n")
                    self.log.see(tk.END)
                    self.log.configure(state=tk.DISABLED)
                except Exception:
                    pass
        except queue.Empty:
            pass
        self.after(200, self._poll_messages)

    def _on_open(self) -> None:
        username = self.username_var.get().strip()
        password = self.password_var.get()
        if not username or not password:
            messagebox.showerror("Faltan datos", "Debes ingresar Usuario y Contraseña.")
            return

        if self._session_thread and self._session_thread.is_alive():
            messagebox.showinfo("Ya está abierto", "La sesión ya está ejecutándose.")
            return

        self.open_btn.config(state=tk.DISABLED)
        self.username_entry.config(state=tk.DISABLED)
        self.password_entry.config(state=tk.DISABLED)

        self._session = SplynxSession(config=self._config, message_sink=self._send)
        self._session_thread = threading.Thread(target=self._session.run, args=(username, password), daemon=True)
        self._session_thread.start()

        self._send("Abriendo Edge y cargando Splynx...")

        # Habilitamos extracción; si presionas antes de estar listo, el bot te avisará.
        self.extract1_btn.config(state=tk.NORMAL)
        self.extract2_btn.config(state=tk.NORMAL)
        self.enrich_btn.config(state=tk.NORMAL)

    def _on_extract1(self) -> None:
        if not self._session:
            return
        self._send(
            "Tabla 1: el bot irá a Tickets > List. Luego tú colocas filtros y presionas 'Aplicar' manualmente; "
            "cuando la tabla recargue, empieza la extracción y paginación."
        )
        self._session.request_extract(table_key="table1", mode="manual")

    def _on_extract2(self) -> None:
        if not self._session:
            return
        self._session.request_extract(table_key="table2")

    def _on_enrich_missing(self) -> None:
        if not self._session:
            messagebox.showinfo("Sesión requerida", "Primero abre Splynx con el botón 'Abrir Splynx'.")
            return

        if self._enrich_thread and self._enrich_thread.is_alive():
            messagebox.showinfo("En progreso", "La búsqueda/enriquecimiento ya está ejecutándose.")
            return

        root = Path(__file__).resolve().parents[1]
        excel_path = root / "output" / "Datos Splynx.xlsx"

        def _job() -> None:
            try:
                self._send(
                    "Iniciando búsqueda en Splynx para IDs de 'Datos no Encontrados'... "
                    "(cierra 'output/Datos Splynx.xlsx' si lo tienes abierto)"
                )
                self._session.request_enrich_missing(excel_path=str(excel_path))
            except Exception as exc:
                self._send(f"Error iniciando búsqueda/enriquecimiento: {exc}")

        self._enrich_thread = threading.Thread(target=_job, daemon=True)
        self._enrich_thread.start()

    def _on_close(self) -> None:
        if self._session:
            self._session.shutdown()
        self.destroy()

    def _on_merge(self) -> None:
        if self._merge_thread and self._merge_thread.is_alive():
            messagebox.showinfo("En progreso", "La comparación ya está ejecutándose.")
            return

        root = Path(__file__).resolve().parents[1]
        excel_path = root / "output" / "Datos Splynx.xlsx"

        def _job() -> None:
            try:
                self._send("Comparando (Reporter ID/ID Cliente -> ID) y creando hoja 'Datos Completos'...")
                total, joined, not_found = merge_tickets_customers(excel_path)
                self._send(
                    f"OK: Datos Completos creada. Tickets: {total}, coincidencias: {joined}, no encontrados: {not_found}."
                )
            except PermissionError:
                self._send(
                    "Error en comparación: el archivo 'output/Datos Splynx.xlsx' está abierto o bloqueado. "
                    "Ciérralo en Excel y vuelve a intentar."
                )
            except Exception as exc:
                self._send(f"Error en comparación: {exc}")

        self._merge_thread = threading.Thread(target=_job, daemon=True)
        self._merge_thread.start()

    def _on_reorder(self) -> None:
        if (self._merge_thread and self._merge_thread.is_alive()) or (
            self._reorder_thread and self._reorder_thread.is_alive()
        ):
            messagebox.showinfo("En progreso", "Primero espera a que termine la comparación/merge.")
            return

        root = Path(__file__).resolve().parents[1]
        excel_path = root / "output" / "Datos Splynx.xlsx"
        template_path = root / "Tickets WOW Enero 2026 rev-2.xlsx"

        def _job() -> None:
            try:
                self._send("Reordenando 'Datos Completos' según plantilla WOW...")
                rows, cols = reorder_datos_completos_by_template(
                    excel_path=excel_path,
                    template_path=template_path,
                    datos_completos_sheet="Datos Completos",
                    template_sheet=None,
                    keep_extra_columns=True,
                    exclude_columns=["Residencia/Urbanización"],
                )
                self._send(f"OK: 'Datos Completos' reordenado. Filas: {rows}, columnas: {cols}.")
            except PermissionError:
                self._send(
                    "Error al reordenar: el archivo 'output/Datos Splynx.xlsx' está abierto o bloqueado. "
                    "Ciérralo en Excel y vuelve a intentar."
                )
            except Exception as exc:
                self._send(f"Error al reordenar: {exc}")

        self._reorder_thread = threading.Thread(target=_job, daemon=True)
        self._reorder_thread.start()


def main() -> None:
    App().mainloop()


if __name__ == "__main__":
    main()
