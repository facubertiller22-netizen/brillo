import os
import sys
import uvicorn
from pyngrok import ngrok, conf

# Load .env if present
env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_file):
    for line in open(env_file):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

NGROK_TOKEN = os.getenv("NGROK_TOKEN", "")

if not NGROK_TOKEN:
    print("")
    print("=" * 55)
    print("  Falta el token de ngrok.")
    print("  1. Crea cuenta gratis en: ngrok.com/signup")
    print("  2. Copia tu token en:     dashboard.ngrok.com/get-started/your-authtoken")
    print("  3. Corre:")
    print('     NGROK_TOKEN=tu-token python start.py')
    print("=" * 55)
    print("")
    sys.exit(1)

conf.get_default().auth_token = NGROK_TOKEN

import time, re

# Cerrar procesos ngrok previos
try:
    ngrok.kill()
    time.sleep(2)
except Exception:
    pass

public_url = None
try:
    tunnel     = ngrok.connect(8000, "http")
    public_url = tunnel.public_url
except Exception as e:
    err = str(e)
    # Caso 1: el tunel ya esta online, extraemos la URL
    match = re.search(r"https://[\w\-]+\.ngrok-free\.\w+", err)
    if match:
        public_url = match.group(0)
        print("  (Usando tunel ngrok ya activo)")
    # Caso 2: limite de sesiones u otro error de ngrok
    else:
        public_url = "http://localhost:8000"
        print("")
        print("  AVISO: ngrok no pudo iniciarse.")
        print("  Causa: " + err.split("\n")[0][:80])
        print("  Solucion: cerrá las sesiones en dashboard.ngrok.com/agents")
        print("  El servidor igual corre en LOCAL: http://localhost:8000")
        print("")

print("")
print("=" * 55)
print("  Lavadero Nordelta a Domicilio")
print("=" * 55)
print(f"  Clientes:  {public_url}/")
print(f"  Admin:     {public_url}/admin")
print(f"  Local:     http://localhost:8000/")
print("=" * 55)
print("")

os.environ["BASE_URL"] = public_url
uvicorn.run("main:app", host="0.0.0.0", port=8000)
