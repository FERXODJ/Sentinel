$ErrorActionPreference = 'Stop'

# Build Windows .exe for the Tkinter + Playwright app
# Output will be in .\dist\Sentinel-1\Sentinel-1.exe (onedir mode)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$py = Join-Path $root '.venv\Scripts\python.exe'
if (!(Test-Path $py)) {
  throw "No se encontró Python en .venv. Crea el venv e instala requirements.txt primero."
}

# Clean previous builds
Remove-Item -Recurse -Force (Join-Path $root 'build') -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force (Join-Path $root 'dist') -ErrorAction SilentlyContinue
Remove-Item -Force (Join-Path $root 'Sentinel-1.spec') -ErrorAction SilentlyContinue

# Data files to bundle next to the exe
# NOTE: config.json NO se embebe (para que el equipo lo pueda ajustar). Se incluye config.example.json.
$data1 = "config.example.json;."
$data2 = "Tickets WOW Enero 2026 rev-2.xlsx;."

& $py -m PyInstaller `
  --noconsole `
  --onedir `
  --clean `
  --name "Sentinel-1" `
  --add-data $data1 `
  --add-data $data2 `
  --collect-all playwright `
  "run_app.py"

Write-Host "\nOK: build listo en: dist\\Sentinel-1\\Sentinel-1.exe" -ForegroundColor Green
Write-Host "Nota: En la PC destino deben tener Edge instalado." -ForegroundColor Yellow
Write-Host "Si la app usa Playwright con browsers descargados, correr: playwright install" -ForegroundColor Yellow
