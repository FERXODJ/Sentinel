import queue
import threading
import tkinter as tk
from tkinter import messagebox

from .splynx_playwright import SplynxSession
from .util import load_config


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        self.title("Splynx Scraper (Edge + Playwright)")
        self.geometry("520x320")
        self.resizable(False, False)

        self._config = load_config()
        self._messages: "queue.Queue[str]" = queue.Queue()

        self._session: SplynxSession | None = None
        self._session_thread: threading.Thread | None = None

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

        self.status_var = tk.StringVar(value="Listo. Ingresa tus credenciales.")
        self.status = tk.Label(frm, textvariable=self.status_var, anchor="w", justify=tk.LEFT, wraplength=480)
        self.status.grid(row=5, column=0, columnspan=3, sticky="we")

        note = (
            "Flujo: el bot abre Edge y llena usuario/clave. "
            "Luego tú ingresas el 2FA y haces Login manualmente. "
            "Cuando estés en la pantalla correcta, presiona Extraer Tabla 1 o 2."
        )
        tk.Label(frm, text=note, fg="#444", wraplength=480, justify=tk.LEFT).grid(
            row=6, column=0, columnspan=3, sticky="we", pady=(pad, 0)
        )

        for c in range(3):
            frm.grid_columnconfigure(c, weight=1)

    def _send(self, msg: str) -> None:
        self._messages.put(msg)

    def _poll_messages(self) -> None:
        try:
            while True:
                msg = self._messages.get_nowait()
                self.status_var.set(msg)
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

    def _on_extract1(self) -> None:
        if not self._session:
            return
        self._session.request_extract(table_key="table1")

    def _on_extract2(self) -> None:
        if not self._session:
            return
        self._session.request_extract(table_key="table2")

    def _on_close(self) -> None:
        if self._session:
            self._session.shutdown()
        self.destroy()


def main() -> None:
    App().mainloop()


if __name__ == "__main__":
    main()
