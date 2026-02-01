# Splynx Scraper (Edge + Playwright)

Este proyecto abre Splynx en **Microsoft Edge** usando **Playwright**, autocompleta usuario/clave desde una **IU** (Tkinter), te deja hacer **2FA y Login manual**, y luego permite extraer **dos tablas** a CSV.

## Requisitos
- Windows con Microsoft Edge instalado
- Python 3.9+ (recomendado 3.10/3.11)

## Instalación
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
playwright install
```

> Nota: Usamos `channel=msedge` para abrir Edge. El comando `playwright install` instala los componentes necesarios de Playwright.

## Configuración
Copia `config.example.json` a `config.json` y ajusta selectores si hace falta:
- `selectors.username` (por defecto `#login`)
- `selectors.password` (por defecto `#password`)
- `tables.table1.selector` / `tables.table2.selector` (selector de la tabla a extraer)

## Ejecutar
```bash
python -m src.app
```

## Uso
1. Escribe usuario y contraseña en la IU.
2. Click **Abrir Splynx** (abre Edge y llena usuario/clave).
3. Completa **2FA** y presiona **Login** manualmente en el navegador.
4. Navega a la pantalla donde esté la tabla y luego click **Extraer Tabla 1**.
5. Repite para **Tabla 2**.

Los CSV salen en `output/`.
