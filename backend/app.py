
import hashlib, secrets, sqlite3, threading
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 折映双正式切断口径（见 docs/折映切断说明.md，2026-11-20 锁定）：
# 乙路径 = 同新键只保留一行正式，第二个及以后的来源写入来源旁表，正式与旁表同事务。
# 只允许“乙”，改成其它值即拒写并记“折映偏了”。
CUT_PATH = "乙"

# 同新键折映须占键锁；后到者只记争用，不写正式、不写来源旁表。
_KEY_LOCKS: dict[str, threading.Lock] = {}
_KEY_LOCKS_GUARD = threading.Lock()

def key_lock(new_key: str) -> threading.Lock:
    with _KEY_LOCKS_GUARD:
        return _KEY_LOCKS.setdefault(new_key, threading.Lock())

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "实体键桥"
SKELETON = "实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def _has_receipt(conn, old_key, entity_key, version) -> bool:
    return conn.execute(
        "SELECT 1 FROM remap_receipts WHERE old_key=? AND entity_key=? AND version=? AND status='落库' LIMIT 1",
        (old_key, entity_key, version),
    ).fetchone() is not None

def _backfill_source_and_receipt(conn, entity_key, old_key, version, created_at, detail):
    conn.execute(
        "INSERT OR IGNORE INTO formal_sources(entity_key,old_key,version,created_at) VALUES (?,?,?,?)",
        (entity_key, old_key, version, created_at),
    )
    if not _has_receipt(conn, old_key, entity_key, version):
        conn.execute(
            "INSERT INTO remap_receipts(old_key,entity_key,version,status,detail,created_at) VALUES (?,?,?,?,?,?)",
            (old_key, entity_key, version, "落库", detail, created_at),
        )

def _migrate_single_formal(conn):
    """把历史遗留的同新键多行正式折成单行，并把每个正式来源补进旁表与回执。

    满足五处同态：正式列表、来源旁表、映射册、互证卡、折映回执。
    可重复执行（幂等）。
    """
    dups = conn.execute(
        "SELECT entity_key, COUNT(*) c FROM formal_entities GROUP BY entity_key HAVING c > 1"
    ).fetchall()
    for d in dups:
        key = d["entity_key"]
        rows = conn.execute(
            "SELECT * FROM formal_entities WHERE entity_key=? ORDER BY id", (key,)
        ).fetchall()
        for extra in rows[1:]:
            _backfill_source_and_receipt(
                conn, key, extra["mapped_from"], extra["version"], extra["created_at"], "迁移折并补录"
            )
            conn.execute("DELETE FROM formal_entities WHERE id=?", (extra["id"],))

    # 每个正式行的主来源都必须能在旁表与回执中查到。
    for r in conn.execute("SELECT * FROM formal_entities ORDER BY id").fetchall():
        _backfill_source_and_receipt(
            conn, r["entity_key"], r["mapped_from"], r["version"], r["created_at"], "迁移补录"
        )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_formal_entity_key ON formal_entities(entity_key)")

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS map_versions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          status TEXT NOT NULL,
          effective_from TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS mappings(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT NOT NULL,
          old_key TEXT NOT NULL,
          new_key TEXT NOT NULL,
          kind TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS formal_entities(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          entity_key TEXT NOT NULL,
          mapped_from TEXT NOT NULL,
          version TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS formal_sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          entity_key TEXT NOT NULL,
          old_key TEXT NOT NULL,
          version TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(entity_key, old_key)
        );
        CREATE TABLE IF NOT EXISTS remap_receipts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          old_key TEXT NOT NULL,
          entity_key TEXT NOT NULL,
          version TEXT NOT NULL,
          status TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS conflicts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          old_key TEXT NOT NULL,
          reason TEXT NOT NULL,
          status TEXT NOT NULL
        );
        """)
        _migrate_single_formal(conn)
        if conn.execute("SELECT COUNT(*) c FROM map_versions").fetchone()["c"] == 0:
            conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)", ("m-2026.11","生效","2026-11-01"))
            conn.executemany("INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)", [
                ("m-2026.11","OLD-A1","NEW-100","一对一"),
                ("m-2026.11","OLD-B1","NEW-200","多对一"),
                ("m-2026.11","OLD-B2","NEW-200","多对一"),
            ])
            conn.execute("INSERT INTO formal_entities(entity_key,mapped_from,version,created_at) VALUES (?,?,?,?)",
                         ("NEW-100","OLD-A1","m-2026.11","2026-11-10T09:00:00"))
            conn.execute("INSERT INTO formal_sources(entity_key,old_key,version,created_at) VALUES (?,?,?,?)",
                         ("NEW-100","OLD-A1","m-2026.11","2026-11-10T09:00:00"))
            conn.execute("INSERT INTO remap_receipts(old_key,entity_key,version,status,detail,created_at) VALUES (?,?,?,?,?,?)",
                         ("OLD-A1","NEW-100","m-2026.11","落库","折入正式区","2026-11-10T09:00:00"))
            conn.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)", ("OLD-C9","无映射","挂起"))

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM mappings").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "mappings": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/map-versions")
def versions(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM map_versions ORDER BY id").fetchall()]}

@app.get("/api/mappings")
def mappings(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM mappings ORDER BY id").fetchall()]}

class RemapIn(BaseModel):
    old_key: str

def _record_source(conn, entity_key: str, old_key: str, version: str, now: str) -> None:
    """来源旁表写入。与正式行同事务；失败由调用方回滚整笔。"""
    conn.execute(
        "INSERT OR IGNORE INTO formal_sources(entity_key,old_key,version,created_at) VALUES (?,?,?,?)",
        (entity_key, old_key, version, now),
    )

def _write_conflict(conn, old_key: str, reason: str, status: str) -> None:
    conn.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)", (old_key, reason, status))

def _write_receipt(conn, old_key, entity_key, version, status, detail, now):
    conn.execute(
        "INSERT INTO remap_receipts(old_key,entity_key,version,status,detail,created_at) VALUES (?,?,?,?,?,?)",
        (old_key, entity_key, version, status, detail, now),
    )

@app.post("/api/remap")
def remap(body: RemapIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = datetime.now().isoformat(timespec="seconds")

    # 偏离保护：切断口径锁定乙路径，任何偏离一律拒写并留痕。
    if CUT_PATH != "乙":
        with connect() as c:
            _write_conflict(c, body.old_key, "折映偏了", "挂起")
            _write_receipt(c, body.old_key, "", "", "拒绝", "折映偏了", now)
            c.commit()
        raise HTTPException(409, "折映口径偏离乙路径，已拒写并记折映偏了")

    with connect() as c:
        ver = c.execute("SELECT version FROM map_versions WHERE status='生效' ORDER BY id DESC LIMIT 1").fetchone()
        if not ver: raise HTTPException(404, "无生效映射版本")
        m = c.execute("SELECT * FROM mappings WHERE version=? AND old_key=?", (ver["version"], body.old_key)).fetchone()
        if not m:
            _write_conflict(c, body.old_key, "无映射", "挂起")
            _write_receipt(c, body.old_key, "", ver["version"], "拒绝", "无映射", now)
            c.commit()
            raise HTTPException(409, "无映射，已进冲突队列")
        new_key = m["new_key"]

        lock = key_lock(new_key)
        if not lock.acquire(blocking=False):
            # 后到者只记争用：不改正式、不写来源旁表。
            _write_conflict(c, body.old_key, "争用", "挂起")
            _write_receipt(c, body.old_key, new_key, ver["version"], "争用", "抢键锁失败，后到记争用", now)
            c.commit()
            raise HTTPException(409, "同名新键正在折入，后到记争用")

        try:
            existing = c.execute(
                "SELECT * FROM formal_entities WHERE entity_key=? ORDER BY id LIMIT 1", (new_key,)
            ).fetchone()
            try:
                if existing is None:
                    # 乙路径：正式只留一行。正式行与来源旁表同一事务；任一步失败整笔回滚。
                    c.execute(
                        "INSERT INTO formal_entities(entity_key,mapped_from,version,created_at) VALUES (?,?,?,?)",
                        (new_key, body.old_key, ver["version"], now),
                    )
                # 首个来源与后续来源都写入旁表；同新键后续折入时正式零增。
                _record_source(c, new_key, body.old_key, ver["version"], now)
                _write_receipt(c, body.old_key, new_key, ver["version"], "落库", "折入正式区", now)
                c.commit()
            except sqlite3.IntegrityError:
                # 多进程下唯一索引兜底：视同抢锁失败，只记争用。
                c.rollback()
                _write_conflict(c, body.old_key, "争用", "挂起")
                _write_receipt(c, body.old_key, new_key, ver["version"], "争用", "唯一索引兜底，记争用", now)
                c.commit()
                raise HTTPException(409, "同名新键已被占用，后到记争用")
            return {"entity_key": new_key, "version": ver["version"], "status": "落库"}
        finally:
            lock.release()

@app.get("/api/formal")
def formal(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM formal_entities ORDER BY id DESC").fetchall()
        items = []
        for r in rows:
            item = dict(r)
            item["sources"] = [
                dict(s) for s in c.execute(
                    "SELECT old_key,version,created_at FROM formal_sources WHERE entity_key=? ORDER BY id",
                    (r["entity_key"],),
                ).fetchall()
            ]
            items.append(item)
        return {"items": items}

@app.get("/api/formal-sources")
def formal_sources(entity_key: str | None = None, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        if entity_key:
            rows = c.execute(
                "SELECT * FROM formal_sources WHERE entity_key=? ORDER BY id", (entity_key,)
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM formal_sources ORDER BY id").fetchall()
        return {"items": [dict(r) for r in rows]}

@app.get("/api/remap-receipts")
def remap_receipts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM remap_receipts ORDER BY id DESC").fetchall()]}

@app.get("/api/conflicts")
def conflicts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM conflicts ORDER BY id DESC").fetchall()]}

@app.get("/api/verification")
def verification(authorization: str | None = Header(default=None)):
    """互证卡：五处同态核对，任一不一致即 ok=False。"""
    require_user(authorization)
    with connect() as c:
        formal = c.execute("SELECT entity_key, mapped_from FROM formal_entities ORDER BY id").fetchall()
        sources = c.execute("SELECT entity_key, old_key FROM formal_sources ORDER BY id").fetchall()
        mappings = c.execute("SELECT old_key, new_key FROM mappings ORDER BY id").fetchall()
        receipts = c.execute(
            "SELECT old_key, entity_key, status FROM remap_receipts ORDER BY id"
        ).fetchall()

        dup_keys = c.execute(
            "SELECT entity_key FROM formal_entities GROUP BY entity_key HAVING COUNT(*) > 1"
        ).fetchall()

        formal_keys = {r["entity_key"] for r in formal}
        source_by_key: dict[str, set] = {}
        for s in sources:
            source_by_key.setdefault(s["entity_key"], set()).add(s["old_key"])
        mapping_by_key: dict[str, set] = {}
        for m in mappings:
            mapping_by_key.setdefault(m["new_key"], set()).add(m["old_key"])
        receipt_by_key: dict[str, set] = {}
        for r in receipts:
            if r["status"] == "落库":
                receipt_by_key.setdefault(r["entity_key"], set()).add(r["old_key"])

        # 每个正式键的来源必须被映射册覆盖，且落库回执必须覆盖这些来源。
        source_in_ledger = all(
            srcs <= mapping_by_key.get(key, set()) for key, srcs in source_by_key.items()
        )
        receipts_match = all(
            receipt_by_key.get(key, set()) == srcs for key, srcs in source_by_key.items()
        )
        # 正式行自身的 mapped_from 必须出现在旁表中。
        formal_primary_in_sources = all(
            r["mapped_from"] in source_by_key.get(r["entity_key"], set()) for r in formal
        )
        formal_keys_for_new_200 = sum(1 for r in formal if r["entity_key"] == "NEW-200")

        return {
            "ok": not dup_keys and source_in_ledger and receipts_match and formal_primary_in_sources,
            "checks": {
                "single_formal_per_key": not dup_keys,
                "source_in_mapping_ledger": source_in_ledger,
                "receipts_cover_sources": receipts_match,
                "formal_primary_in_sources": formal_primary_in_sources,
            },
            "counts": {
                "formal_rows": len(formal),
                "formal_keys": len(formal_keys),
                "source_rows": len(sources),
                "formal_keys_for_new_200": formal_keys_for_new_200,
            },
        }

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
