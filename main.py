"""
Brillo – Business Backend
=========================
TWO service layers:
  • STANDARD  – fixed pricing, we assign any provider, we keep revenue
  • PRO       – provider keeps own pricing, we take a commission %

Database: PostgreSQL (via DATABASE_URL env var)
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, Literal
from datetime import datetime
from contextlib import contextmanager
import os, secrets, mercadopago
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Brillo", version="3.0")

DATABASE_URL = os.getenv("DATABASE_URL", "")
MP_TOKEN     = os.getenv("MP_ACCESS_TOKEN", "")
BASE_URL     = os.getenv("BASE_URL", "https://brillo.onrender.com")

# ── Database ──────────────────────────────────────────────────────────────────

@contextmanager
def db():
    """Open a connection, yield a RealDictCursor, commit on success, rollback on error."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _col_exists(cur, table: str, column: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
    """, (table, column))
    return cur.fetchone() is not None


def _add_column(cur, table: str, column: str, definition: str):
    """Add a column only if it does not already exist (PostgreSQL safe)."""
    if not _col_exists(cur, table, column):
        cur.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')


def init_db():
    with db() as c:
        # ── services ─────────────────────────────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS services (
                id          SERIAL PRIMARY KEY,
                name        TEXT    NOT NULL,
                description TEXT    NOT NULL DEFAULT '',
                fixed_price FLOAT   NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT    NOT NULL
            )
        """)

        # ── providers ────────────────────────────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS providers (
                id             SERIAL PRIMARY KEY,
                name           TEXT    NOT NULL,
                phone          TEXT    NOT NULL,
                zone           TEXT    NOT NULL DEFAULT '',
                bio            TEXT    NOT NULL DEFAULT '',
                is_pro         INTEGER NOT NULL DEFAULT 0,
                commission_pct FLOAT   NOT NULL DEFAULT 20.0,
                availability   TEXT    NOT NULL DEFAULT 'available',
                notes          TEXT    NOT NULL DEFAULT '',
                provider_token TEXT,
                noshow_count   INTEGER NOT NULL DEFAULT 0,
                created_at     TEXT    NOT NULL
            )
        """)

        # ── requests ─────────────────────────────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id                   SERIAL PRIMARY KEY,
                name                 TEXT    NOT NULL,
                phone                TEXT    NOT NULL,
                address              TEXT    NOT NULL,
                service_type         TEXT    NOT NULL DEFAULT 'standard',
                selected_service_id  INTEGER,
                selected_provider_id INTEGER,
                status               TEXT    NOT NULL DEFAULT 'pending',
                final_price          FLOAT,
                commission_amount    FLOAT,
                provider_payout      FLOAT,
                payment_status       TEXT    NOT NULL DEFAULT 'unpaid',
                payment_link         TEXT,
                scheduled_date       TEXT,
                scheduled_time       TEXT,
                client_token         TEXT,
                provider_confirmation TEXT   NOT NULL DEFAULT 'pending',
                noshow               INTEGER NOT NULL DEFAULT 0,
                created_at           TEXT    NOT NULL
            )
        """)

        # ── safe migrations for any existing older tables ─────────────────────
        for col, dfn in [
            ("is_pro",               "INTEGER NOT NULL DEFAULT 0"),
            ("commission_pct",       "FLOAT   NOT NULL DEFAULT 20.0"),
            ("availability",         "TEXT    NOT NULL DEFAULT 'available'"),
            ("notes",                "TEXT    NOT NULL DEFAULT ''"),
            ("provider_token",       "TEXT"),
            ("noshow_count",         "INTEGER NOT NULL DEFAULT 0"),
            ("specialties",          "TEXT    NOT NULL DEFAULT ''"),
        ]:
            _add_column(c, "providers", col, dfn)

        for col, dfn in [
            ("service_type",          "TEXT NOT NULL DEFAULT 'standard'"),
            ("selected_service_id",   "INTEGER"),
            ("selected_provider_id",  "INTEGER"),
            ("final_price",           "FLOAT"),
            ("commission_amount",     "FLOAT"),
            ("provider_payout",       "FLOAT"),
            ("scheduled_date",        "TEXT"),
            ("scheduled_time",        "TEXT"),
            ("client_token",          "TEXT"),
            ("provider_confirmation", "TEXT NOT NULL DEFAULT 'pending'"),
            ("noshow",                "INTEGER NOT NULL DEFAULT 0"),
            ("vehicle_type",          "TEXT"),
            ("notes",                 "TEXT"),
        ]:
            _add_column(c, "requests", col, dfn)

init_db()


def _seed_provider_tokens():
    """Assign a unique portal token to every provider that doesn't have one yet."""
    with db() as c:
        c.execute("SELECT id FROM providers WHERE provider_token IS NULL OR provider_token=''")
        rows = c.fetchall()
        for row in rows:
            token = secrets.token_urlsafe(16)
            c.execute("UPDATE providers SET provider_token=%s WHERE id=%s", (token, row["id"]))

_seed_provider_tokens()


# ── Pydantic Models ───────────────────────────────────────────────────────────

class NewService(BaseModel):
    name:        str
    description: str   = ""
    fixed_price: float

class UpdateService(BaseModel):
    name:        Optional[str]   = None
    description: Optional[str]  = None
    fixed_price: Optional[float]= None
    active:      Optional[int]  = None

class NewProvider(BaseModel):
    name:           str
    phone:          str
    zone:           str
    bio:            str   = ""
    is_pro:         bool  = False
    commission_pct: float = 20.0
    availability:   str   = "available"
    notes:          str   = ""
    specialties:    str   = ""

class UpdateProvider(BaseModel):
    name:           Optional[str]   = None
    phone:          Optional[str]   = None
    zone:           Optional[str]   = None
    bio:            Optional[str]   = None
    is_pro:         Optional[bool]  = None
    commission_pct: Optional[float] = None
    availability:   Optional[str]   = None
    notes:          Optional[str]   = None
    specialties:    Optional[str]   = None

class NewRequest(BaseModel):
    name:    str
    phone:   str
    address: str
    service_type:         Literal["standard", "pro"]
    selected_service_id:  Optional[int] = None
    selected_provider_id: Optional[int] = None
    scheduled_date:  Optional[str] = None
    scheduled_time:  Optional[str] = None
    vehicle_type:    Optional[str] = None
    notes:           Optional[str] = None

class AvailabilityBody(BaseModel):
    availability: str

class RescheduleBody(BaseModel):
    scheduled_date: str
    scheduled_time: str

class AssignBody(BaseModel):
    provider_id: int

class StatusBody(BaseModel):
    status: Literal["pending", "assigned", "completed"]

class PaymentBody(BaseModel):
    price: float


# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _calculate_financials(price: float, service_type: str, provider_id: Optional[int]) -> dict:
    if service_type == "pro" and provider_id:
        with db() as c:
            c.execute("SELECT commission_pct FROM providers WHERE id=%s", (provider_id,))
            prov = c.fetchone()
        pct        = float(prov["commission_pct"]) if prov else 20.0
        commission = round(price * pct / 100, 2)
        payout     = round(price - commission, 2)
        log(f"[FINANCIERO] Pro | ${price} | Com {pct}% = ${commission} | Lavador ${payout}")
    else:
        commission = 0.0
        payout     = 0.0
        log(f"[FINANCIERO] Standard | ${price} | Ingreso nuestro")
    return {"final_price": price, "commission_amount": commission, "provider_payout": payout}


# ── Services ──────────────────────────────────────────────────────────────────

@app.post("/api/services", status_code=201, tags=["Services"])
def create_service(b: NewService):
    now = datetime.utcnow().isoformat()
    with db() as c:
        c.execute(
            "INSERT INTO services (name, description, fixed_price, created_at) "
            "VALUES (%s,%s,%s,%s) RETURNING id",
            (b.name, b.description, b.fixed_price, now)
        )
        sid = c.fetchone()["id"]
    log(f"[SERVICIO] #{sid} '{b.name}' | ${b.fixed_price}")
    return {"id": sid, "name": b.name, "fixed_price": b.fixed_price}


@app.get("/api/services", tags=["Services"])
def list_services(active_only: bool = False):
    with db() as c:
        sql = "SELECT * FROM services" + (" WHERE active=1" if active_only else "") + " ORDER BY id"
        c.execute(sql)
        return [dict(r) for r in c.fetchall()]


@app.patch("/api/services/{sid}", tags=["Services"])
def update_service(sid: int, b: UpdateService):
    fields = {k: v for k, v in b.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(400, "No hay campos para actualizar")
    set_clause = ", ".join(f"{k}=%s" for k in fields)
    with db() as c:
        c.execute(f"UPDATE services SET {set_clause} WHERE id=%s", (*fields.values(), sid))
        if c.rowcount == 0:
            raise HTTPException(404, "Servicio no encontrado")
    log(f"[SERVICIO] Actualizado #{sid}")
    return {"id": sid, **fields}


@app.delete("/api/services/{sid}", tags=["Services"])
def delete_service(sid: int):
    with db() as c:
        c.execute("UPDATE services SET active=0 WHERE id=%s", (sid,))
        if c.rowcount == 0:
            raise HTTPException(404, "Servicio no encontrado")
    log(f"[SERVICIO] Desactivado #{sid}")
    return {"id": sid, "active": False}


# ── Providers ─────────────────────────────────────────────────────────────────

@app.post("/api/providers", status_code=201, tags=["Providers"])
def create_provider(b: NewProvider):
    now   = datetime.utcnow().isoformat()
    token = secrets.token_urlsafe(16)
    with db() as c:
        c.execute(
            """INSERT INTO providers
               (name, phone, zone, bio, is_pro, commission_pct, availability, notes, specialties, provider_token, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (b.name, b.phone, b.zone, b.bio,
             int(b.is_pro), b.commission_pct, b.availability, b.notes, b.specialties, token, now)
        )
        pid = c.fetchone()["id"]
    portal_url = f"/proveedor/{token}"
    log(f"[LAVADOR] #{pid} {b.name} | {'PRO' if b.is_pro else 'Std'} | Com {b.commission_pct}% | Token: {token}")
    return {"id": pid, "name": b.name, "is_pro": b.is_pro,
            "commission_pct": b.commission_pct, "provider_token": token,
            "portal_url": portal_url}


@app.get("/api/providers", tags=["Providers"])
def list_providers(pro_only: bool = False):
    with db() as c:
        sql = "SELECT * FROM providers" + (" WHERE is_pro=1" if pro_only else "") + " ORDER BY id DESC"
        c.execute(sql)
        return [dict(r) for r in c.fetchall()]


_PUBLIC_PROVIDER_FIELDS = "id, name, zone, bio, is_pro, availability, specialties"

@app.get("/api/public/providers", tags=["Public"])
def public_providers():
    with db() as c:
        c.execute(
            f"SELECT {_PUBLIC_PROVIDER_FIELDS} FROM providers "
            f"WHERE is_pro=1 AND availability != 'inactive' ORDER BY id DESC"
        )
        return [dict(r) for r in c.fetchall()]


@app.get("/api/public/services", tags=["Public"])
def public_services():
    with db() as c:
        c.execute("SELECT id, name, description, fixed_price FROM services WHERE active=1 ORDER BY id")
        return [dict(r) for r in c.fetchall()]


@app.patch("/api/providers/{pid}", tags=["Providers"])
def update_provider(pid: int, b: UpdateProvider):
    data = b.model_dump(exclude_none=True)
    if "is_pro" in data:
        data["is_pro"] = int(data["is_pro"])
    if not data:
        raise HTTPException(400, "No hay campos para actualizar")
    set_clause = ", ".join(f"{k}=%s" for k in data)
    with db() as c:
        c.execute(f"UPDATE providers SET {set_clause} WHERE id=%s", (*data.values(), pid))
        if c.rowcount == 0:
            raise HTTPException(404, "Lavador no encontrado")
    log(f"[LAVADOR] Actualizado #{pid}")
    return {"id": pid, **data}


# ── Requests ──────────────────────────────────────────────────────────────────

@app.post("/api/requests", status_code=201, tags=["Requests"])
def create_request(b: NewRequest):
    if b.service_type == "pro" and not b.selected_provider_id:
        raise HTTPException(400, "pro requests require selected_provider_id")

    service_name  = None
    initial_price = None

    if b.service_type == "standard" and b.selected_service_id:
        with db() as c:
            c.execute("SELECT * FROM services WHERE id=%s AND active=1", (b.selected_service_id,))
            svc = c.fetchone()
        if not svc:
            raise HTTPException(404, "Servicio no encontrado o inactivo")
        svc           = dict(svc)
        service_name  = svc["name"]
        initial_price = svc["fixed_price"]

    if b.service_type == "pro":
        with db() as c:
            c.execute("SELECT id FROM providers WHERE id=%s AND is_pro=1", (b.selected_provider_id,))
            if not c.fetchone():
                raise HTTPException(404, "Lavador PRO no encontrado")

    now          = datetime.utcnow().isoformat()
    client_token = secrets.token_urlsafe(16)
    with db() as c:
        c.execute(
            """INSERT INTO requests
               (name, phone, address, service_type, selected_service_id, selected_provider_id,
                final_price, scheduled_date, scheduled_time, client_token,
                vehicle_type, notes, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (b.name, b.phone, b.address, b.service_type,
             b.selected_service_id, b.selected_provider_id,
             initial_price, b.scheduled_date, b.scheduled_time, client_token,
             b.vehicle_type, b.notes, now)
        )
        rid = c.fetchone()["id"]

    sched = f" | {b.scheduled_date} {b.scheduled_time}" if b.scheduled_date else ""
    log(f"[PEDIDO] #{rid} | {b.service_type.upper()} | {b.name} | {b.address}{sched}")
    return {"id": rid, "service_type": b.service_type, "status": "pending",
            "final_price": initial_price, "client_token": client_token}


@app.get("/api/requests", tags=["Requests"])
def list_requests(status: Optional[str] = None, service_type: Optional[str] = None):
    conditions, params = [], []
    if status:
        conditions.append("status=%s"); params.append(status)
    if service_type:
        conditions.append("service_type=%s"); params.append(service_type)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    with db() as c:
        c.execute(f"SELECT * FROM requests {where} ORDER BY id DESC", params)
        return [dict(r) for r in c.fetchall()]


@app.patch("/api/requests/{rid}/assign", tags=["Requests"])
def assign_request(rid: int, b: AssignBody):
    with db() as c:
        c.execute("SELECT * FROM requests WHERE id=%s", (rid,))
        req = c.fetchone()
        if not req:
            raise HTTPException(404, "Pedido no encontrado")
        c.execute("SELECT * FROM providers WHERE id=%s", (b.provider_id,))
        prov = c.fetchone()
        if not prov:
            raise HTTPException(404, "Lavador no encontrado")
        c.execute(
            "UPDATE requests SET selected_provider_id=%s, status='assigned' WHERE id=%s",
            (b.provider_id, rid)
        )
    req  = dict(req)
    prov = dict(prov)
    log(f"[ASIGNADO] #{rid} -> {prov['name']} ({prov['phone']})")
    _notify_customer(req, prov)
    _notify_provider(req, prov)
    return {"id": rid, "provider_id": b.provider_id, "status": "assigned"}


@app.patch("/api/requests/{rid}/status", tags=["Requests"])
def set_status(rid: int, b: StatusBody):
    with db() as c:
        c.execute("UPDATE requests SET status=%s WHERE id=%s", (b.status, rid))
        if c.rowcount == 0:
            raise HTTPException(404, "Pedido no encontrado")
    log(f"[STATUS] #{rid} -> {b.status}")
    return {"id": rid, "status": b.status}


@app.post("/api/requests/{rid}/noshow", tags=["Requests"])
def mark_noshow(rid: int):
    with db() as c:
        c.execute("SELECT * FROM requests WHERE id=%s", (rid,))
        req = c.fetchone()
        if not req:
            raise HTTPException(404, "Pedido no encontrado")
        req = dict(req)
        c.execute(
            "UPDATE requests SET noshow=1, status='pending', "
            "selected_provider_id=NULL, provider_confirmation='pending' WHERE id=%s",
            (rid,)
        )
        if req.get("selected_provider_id"):
            c.execute(
                "UPDATE providers SET noshow_count = noshow_count + 1 WHERE id=%s",
                (req["selected_provider_id"],)
            )
    log(f"[NO-SHOW] #{rid} | Lavador #{req.get('selected_provider_id')} → pedido reabierto")
    return {"id": rid, "noshow": True, "status": "pending",
            "message": "Pedido reabierto. El lavador recibió un registro de no-show."}


# ── Payment ───────────────────────────────────────────────────────────────────

@app.post("/api/requests/{rid}/payment", tags=["Payment"])
def create_payment(rid: int, b: PaymentBody):
    if not MP_TOKEN:
        raise HTTPException(500, "MP_ACCESS_TOKEN no configurado")

    with db() as c:
        c.execute("SELECT * FROM requests WHERE id=%s", (rid,))
        req = c.fetchone()
    if not req:
        raise HTTPException(404, "Pedido no encontrado")
    req   = dict(req)
    price = b.price

    if req["service_type"] == "standard" and not b.price:
        with db() as c:
            c.execute("SELECT fixed_price FROM services WHERE id=%s", (req["selected_service_id"],))
            svc = c.fetchone()
        if not svc:
            raise HTTPException(400, "Servicio no encontrado para calcular precio")
        price = svc["fixed_price"]

    financials = _calculate_financials(price, req["service_type"], req["selected_provider_id"])

    sdk  = mercadopago.SDK(MP_TOKEN)
    pref = sdk.preference().create({
        "items": [{"title": f"Brillo – {req.get('address','')}",
                   "quantity": 1, "unit_price": price, "currency_id": "ARS"}],
        "payer": {"name": req["name"]},
        "external_reference": str(rid),
        "back_urls": {"success": f"{BASE_URL}/pago/ok", "failure": f"{BASE_URL}/pago/error"},
        "notification_url": f"{BASE_URL}/api/webhook",
    })
    if pref["status"] != 201:
        raise HTTPException(500, f"Error Mercado Pago: {pref}")

    link = pref["response"]["init_point"]
    with db() as c:
        c.execute(
            """UPDATE requests
               SET final_price=%s, commission_amount=%s, provider_payout=%s,
                   payment_link=%s, payment_status='unpaid'
               WHERE id=%s""",
            (financials["final_price"], financials["commission_amount"],
             financials["provider_payout"], link, rid)
        )
    log(f"[PAGO] #{rid} | ${price} | Com ${financials['commission_amount']} | Link: {link}")
    return {"id": rid, **financials, "payment_link": link}


@app.post("/api/webhook", tags=["Payment"])
async def webhook(req: dict):
    if req.get("type") != "payment":
        return {"ok": True}
    pid = req.get("data", {}).get("id")
    if not pid:
        return {"ok": True}
    sdk  = mercadopago.SDK(MP_TOKEN)
    info = sdk.payment().get(pid)
    if info["status"] == 200 and info["response"].get("status") == "approved":
        rid = info["response"].get("external_reference")
        with db() as c:
            c.execute("UPDATE requests SET payment_status='paid' WHERE id=%s", (rid,))
        log(f"[PAGADO] #{rid}")
    return {"ok": True}


# ── Provider Portal ──────────────────────────────────────────────────────────

def _get_provider_by_token(token: str) -> dict:
    with db() as c:
        c.execute("SELECT * FROM providers WHERE provider_token=%s", (token,))
        p = c.fetchone()
    if not p:
        raise HTTPException(404, "Token de lavador no válido")
    return dict(p)

_PROV_JOB_FIELDS = (
    "r.id, r.address, r.service_type, r.selected_service_id, "
    "r.status, r.scheduled_date, r.scheduled_time, "
    "r.provider_confirmation, r.noshow, r.created_at, "
    "r.provider_payout, r.payment_status, "
    "s.name AS service_name, s.fixed_price"
)

@app.get("/api/proveedor/{token}/jobs", tags=["Provider Portal"])
def provider_jobs(token: str):
    prov = _get_provider_by_token(token)
    with db() as c:
        c.execute(
            f"""SELECT {_PROV_JOB_FIELDS}
                FROM requests r
                LEFT JOIN services s ON s.id = r.selected_service_id
                WHERE r.selected_provider_id=%s
                  AND r.status IN ('assigned','completed')
                  AND r.noshow=0
                ORDER BY r.scheduled_date ASC NULLS LAST, r.id DESC""",
            (prov["id"],)
        )
        rows = c.fetchall()
    safe = {k: prov[k] for k in ("id","name","zone","bio","is_pro","availability","noshow_count")}
    return {"provider": safe, "jobs": [dict(r) for r in rows]}


@app.patch("/api/proveedor/{token}/jobs/{rid}/confirm", tags=["Provider Portal"])
def provider_confirm_job(token: str, rid: int):
    prov = _get_provider_by_token(token)
    with db() as c:
        c.execute("SELECT id FROM requests WHERE id=%s AND selected_provider_id=%s", (rid, prov["id"]))
        if not c.fetchone():
            raise HTTPException(404, "Trabajo no encontrado para este lavador")
        c.execute("UPDATE requests SET provider_confirmation='confirmed' WHERE id=%s", (rid,))
    log(f"[CONFIRM] #{prov['id']} {prov['name']} → trabajo #{rid}")
    return {"id": rid, "provider_confirmation": "confirmed"}


@app.delete("/api/providers/{pid}", tags=["Providers"])
def delete_provider(pid: int):
    with db() as c:
        c.execute("SELECT name FROM providers WHERE id=%s", (pid,))
        p = c.fetchone()
        if not p:
            raise HTTPException(404, "Lavador no encontrado")
        c.execute("DELETE FROM providers WHERE id=%s", (pid,))
    log(f"[LAVADOR] Eliminado #{pid} {p['name']}")
    return {"id": pid, "deleted": True}

@app.patch("/api/proveedor/{token}/availability", tags=["Provider Portal"])
def update_provider_availability(token: str, b: AvailabilityBody):
    prov = _get_provider_by_token(token)
    with db() as c:
        c.execute("UPDATE providers SET availability=%s WHERE id=%s", (b.availability, prov["id"]))
    log(f"[DISPONIBILIDAD] {prov['name']} → {b.availability}")
    return {"availability": b.availability}

@app.patch("/api/proveedor/{token}/jobs/{rid}/reschedule", tags=["Provider Portal"])
def provider_reschedule_job(token: str, rid: int, b: RescheduleBody):
    prov = _get_provider_by_token(token)
    with db() as c:
        c.execute("SELECT id FROM requests WHERE id=%s AND selected_provider_id=%s", (rid, prov["id"]))
        if not c.fetchone():
            raise HTTPException(404, "Trabajo no encontrado")
        c.execute(
            "UPDATE requests SET provider_confirmation='reschedule_requested', "
            "scheduled_date=%s, scheduled_time=%s WHERE id=%s",
            (b.scheduled_date, b.scheduled_time, rid)
        )
    log(f"[RESCHEDULE] #{prov['id']} pidió reprogramar #{rid} → {b.scheduled_date} {b.scheduled_time}")
    return {"id": rid, "provider_confirmation": "reschedule_requested",
            "scheduled_date": b.scheduled_date, "scheduled_time": b.scheduled_time}


# ── Client Tracking ───────────────────────────────────────────────────────────

@app.get("/api/track", tags=["Tracking"])
def track_order(phone: str):
    with db() as c:
        c.execute(
            """SELECT r.id, r.address, r.service_type, r.status,
                      r.scheduled_date, r.scheduled_time,
                      r.payment_status, r.provider_confirmation, r.created_at,
                      s.name AS service_name
               FROM requests r
               LEFT JOIN services s ON s.id = r.selected_service_id
               WHERE r.phone=%s AND r.noshow=0
               ORDER BY r.id DESC LIMIT 5""",
            (phone.strip(),)
        )
        rows = c.fetchall()
    if not rows:
        raise HTTPException(404, "No encontramos pedidos con ese número.")
    return [dict(r) for r in rows]


# ── Notifications ─────────────────────────────────────────────────────────────

def _notify_customer(req: dict, prov: dict):
    link = req.get("payment_link") or ""
    pay  = f"\n  Paga aqui: {link}" if link else ""
    log(f"{'='*50}\n[NOTIF→CLIENTE] {req['name']} ({req['phone']})\n"
        f"  Tu turno fue confirmado.\n"
        f"  Un profesional de Brillo irá a tu domicilio.{pay}\n{'='*50}")

def _notify_provider(req: dict, prov: dict):
    log(f"{'='*50}\n[NOTIF→LAVADOR] {prov['name']}\n"
        f"  Nuevo trabajo: {req['address']} | {req['service_type'].upper()}\n"
        f"  No contactes al cliente. Toda comunicación por Brillo.\n{'='*50}")


# ── HTML Pages ────────────────────────────────────────────────────────────────

def _serve_asset(filename: str, media_type: str):
    from fastapi.responses import FileResponse
    path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(path):
        raise HTTPException(404, f"{filename} no encontrado")
    return FileResponse(path, media_type=media_type)

@app.get("/logo.png",         include_in_schema=False)
def serve_logo(): return _serve_asset("logo.png",         "image/png")
@app.get("/hero.jpg",         include_in_schema=False)
def serve_hero(): return _serve_asset("hero.jpg",         "image/jpeg")
@app.get("/fondolanding.png", include_in_schema=False)
def serve_fondo(): return _serve_asset("fondolanding.png", "image/png")
@app.get("/mp.png",    include_in_schema=False)
def serve_mp():   return _serve_asset("mp.png",    "image/png")
@app.get("/visa.png",  include_in_schema=False)
def serve_visa(): return _serve_asset("visa.png",  "image/png")
@app.get("/amex.png",  include_in_schema=False)
def serve_amex(): return _serve_asset("amex.png",  "image/png")

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def home():
    return open(os.path.join(os.path.dirname(__file__), "home.html"), encoding="utf-8").read()

@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
def admin():
    return open(os.path.join(os.path.dirname(__file__), "admin.html"), encoding="utf-8").read()

@app.get("/proveedor/{token}", response_class=HTMLResponse, include_in_schema=False)
def proveedor_portal(token: str):
    with db() as c:
        c.execute("SELECT id FROM providers WHERE provider_token=%s", (token,))
        if not c.fetchone():
            return HTMLResponse("<h2 style='font-family:sans-serif;padding:40px'>Link inválido.</h2>", 404)
    return open(os.path.join(os.path.dirname(__file__), "proveedor.html"), encoding="utf-8").read()

@app.get("/perfil/{pid}", response_class=HTMLResponse, include_in_schema=False)
def perfil(pid: int):
    with db() as c:
        c.execute(f"SELECT {_PUBLIC_PROVIDER_FIELDS} FROM providers WHERE id=%s AND is_pro=1", (pid,))
        p = c.fetchone()
    if not p:
        return HTMLResponse("<h2>No encontrado</h2>", 404)
    p   = dict(p)
    ini = "".join(w[0].upper() for w in p["name"].split()[:2])
    bio = (f'<p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:20px">'
           f'{p["bio"]}</p>') if p["bio"] else ""
    return f"""<!DOCTYPE html><html lang="es"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{p['name']} – Brillo</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:Arial,sans-serif;background:#f0f4f8;display:flex;justify-content:center;padding:40px 16px}}
.card{{background:white;border-radius:20px;padding:36px 28px;width:100%;max-width:380px;
       box-shadow:0 4px 20px rgba(0,0,0,.1);text-align:center}}
.av{{width:88px;height:88px;border-radius:50%;background:#2563eb;color:white;font-size:32px;
     font-weight:bold;display:flex;align-items:center;justify-content:center;margin:0 auto 16px}}
.name{{font-size:22px;font-weight:bold;color:#1a1a2e;margin-bottom:8px}}
.badge{{display:inline-block;background:#fff3cd;color:#856404;font-size:13px;font-weight:bold;
        padding:4px 14px;border-radius:20px;margin-bottom:12px}}
.zone{{color:#888;font-size:14px;margin-bottom:16px}}
.trust{{background:#eff6ff;border-radius:12px;padding:14px;font-size:13px;color:#1e40af;
        text-align:left;line-height:1.6;margin-bottom:12px}}
.note{{background:#f8f9fa;border-radius:12px;padding:14px;font-size:12px;color:#888;text-align:center}}</style>
</head><body><div class="card">
<div class="av">{ini}</div><div class="name">{p['name']}</div>
<div class="badge">⭐ Detailer PRO</div>
<div class="zone">📍 {p['zone']}</div>{bio}
<div class="trust"><b>✓ Verificado por Brillo</b><br>
Evaluado y aceptado por nuestro equipo. Su trabajo tiene seguimiento y garantía.</div>
<div class="note">Para reservar, hacelo a través de Brillo.<br>No se comparte contacto directo.</div>
</div></body></html>"""
