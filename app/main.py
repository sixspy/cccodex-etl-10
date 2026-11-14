from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import BaseModel

from app.config import Config
from app.db import (
    METHOD_CLEAR,
    METHOD_DIRECT,
    POLICY_CLEAR,
    POLICY_FIXED,
    PartitionError,
    backfill_overview,
    begin_partition_write,
    commit_partition_write,
    get_db,
    get_partition_detail,
    get_pipeline,
    get_policy,
    init_db,
    list_pipelines,
    rollback_partition_write,
    run_extract,
    run_load,
    set_backfill_policy,
    write_partition,
)

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
serializer = URLSafeSerializer(Config.SECRET, salt="cc-etl-session")

app = FastAPI(title="cc-etl-lab")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.exception_handler(PartitionError)
def _partition_error_handler(request: Request, exc: PartitionError):
    return JSONResponse(
        {"ok": False, "error": exc.detail, "code": exc.code},
        status_code=exc.status,
    )


def _read_session(request: Request) -> dict:
    raw = request.cookies.get("etl_session")
    if not raw:
        return {}
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else {}
    except BadSignature:
        return {}


def require_user(request: Request) -> str:
    user = _read_session(request).get("user")
    if user != Config.ADMIN_USER:
        raise HTTPException(status_code=401, detail="未登录")
    return user


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/")
def root(request: Request):
    if _read_session(request).get("user") == Config.ADMIN_USER:
        return RedirectResponse("/app/", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login")
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/app/")
def spa_page(request: Request):
    if _read_session(request).get("user") != Config.ADMIN_USER:
        return RedirectResponse("/login", status_code=302)
    return FileResponse(STATIC / "index.html")


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    user = str(body.get("username", "")).strip()
    pwd = str(body.get("password", ""))
    if user != Config.ADMIN_USER or pwd != Config.ADMIN_PASSWORD:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=400)
    resp = JSONResponse({"ok": True, "user": user})
    resp.set_cookie(
        "etl_session",
        serializer.dumps({"user": user}),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("etl_session")
    return resp


@app.get("/api/me")
def api_me(user: str = Depends(require_user)):
    return {"user": user, "lab": Config.LAB_NAME, "domain": "数据工程"}


@app.get("/api/pipelines")
def api_pipelines(user: str = Depends(require_user)):
    with get_db() as conn:
        return {"items": list_pipelines(conn)}


@app.get("/api/pipelines/{pipeline_id}")
def api_pipeline_detail(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = get_pipeline(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/extract")
def api_extract(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = run_extract(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/load")
def api_load(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = run_load(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


# ---------- 请求模型 ----------

class PolicyBody(BaseModel):
    strategy: str
    window_days: int | None = None


class WriteBeginBody(BaseModel):
    partition_key: str
    method: str = METHOD_DIRECT
    writer: str | None = None
    row_count: int | None = None


class WriteCommitBody(BaseModel):
    row_count: int | None = None


# ---------- 回溯策略：本日锁死 ----------

@app.get("/api/pipelines/{pipeline_id}/backfill/policy")
def api_get_policy(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        if not get_pipeline(conn, pipeline_id):
            raise HTTPException(status_code=404, detail="管道不存在")
        policy = get_policy(conn, pipeline_id)
    return {
        "policy": policy,
        "strategy_options": [
            {"value": POLICY_FIXED, "label": "固定窗重落"},
            {"value": POLICY_CLEAR, "label": "先清空再写"},
        ],
        "method_options": [
            {"value": METHOD_DIRECT, "label": "直接重落"},
            {"value": METHOD_CLEAR, "label": "清空再写"},
        ],
    }


@app.put("/api/pipelines/{pipeline_id}/backfill/policy")
def api_set_policy(pipeline_id: int, body: PolicyBody, user: str = Depends(require_user)):
    with get_db() as conn:
        policy = set_backfill_policy(conn, pipeline_id, body.strategy, body.window_days, user)
    return {"ok": True, "policy": policy}


# ---------- 回溯总览（占用板 + 事件 + 策略） ----------

@app.get("/api/pipelines/{pipeline_id}/backfill/overview")
def api_backfill_overview(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        return backfill_overview(conn, pipeline_id)


@app.get("/api/pipelines/{pipeline_id}/partitions/{partition_id}")
def api_partition_detail(pipeline_id: int, partition_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        detail = get_partition_detail(conn, pipeline_id, partition_id)
    if not detail:
        raise HTTPException(status_code=404, detail="分区不存在")
    return detail


# ---------- 分区写入：服务端裁决 ----------

@app.post("/api/pipelines/{pipeline_id}/partitions/write")
def api_write_partition(pipeline_id: int, body: WriteBeginBody, user: str = Depends(require_user)):
    with get_db() as conn:
        result = write_partition(
            conn,
            pipeline_id,
            body.partition_key,
            body.method,
            body.writer or user,
            body.row_count,
        )
    return {"ok": True, **result}


@app.post("/api/pipelines/{pipeline_id}/partitions/begin")
def api_begin_write(pipeline_id: int, body: WriteBeginBody, user: str = Depends(require_user)):
    with get_db() as conn:
        begun = begin_partition_write(
            conn, pipeline_id, body.partition_key, body.method, body.writer or user
        )
    return {"ok": True, **begun}


@app.post("/api/writes/{write_id}/commit")
def api_commit_write(write_id: int, body: WriteCommitBody, user: str = Depends(require_user)):
    with get_db() as conn:
        result = commit_partition_write(conn, write_id, body.row_count)
    return {"ok": True, **result}


@app.post("/api/writes/{write_id}/rollback")
def api_rollback_write(write_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        result = rollback_partition_write(conn, write_id)
    return {"ok": True, **result}


def create_app() -> FastAPI:
    return app
