import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from app.config import Config

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

# 回溯策略
POLICY_FIXED = "fixed_days"   # 只允许重落最近固定天数的分区
POLICY_CLEAR = "clear_first"  # 任意历史分区，但须整分区先清空再写
POLICY_NAMES = {
    POLICY_FIXED: "固定窗重落",
    POLICY_CLEAR: "先清空再写",
}

# 落库手法
METHOD_DIRECT = "direct"        # 直接重落
METHOD_CLEAR = "clear_first"    # 整分区清空再写
METHOD_LABELS = {
    METHOD_DIRECT: "直接重落",
    METHOD_CLEAR: "清空再写",
}

# 写入事件
EV_OPEN = "写入中"
EV_FIRST = "落库成功"
EV_REWRITE = "重写成功"
EV_CLEAR = "清空"
EV_REJECT = "拒绝"
EV_PREEMPT = "抢占失败"
EV_ROLLBACK = "回滚"


class PartitionError(Exception):
    """分区落库被服务端裁决拒绝。"""

    def __init__(self, detail: str, status: int = 400, code: str = "rejected"):
        self.detail = detail
        self.status = status
        self.code = code
        super().__init__(detail)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipelines (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              extracted INTEGER NOT NULL DEFAULT 0,
              loaded INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS backfill_policies (
              pipeline_id INTEGER PRIMARY KEY REFERENCES pipelines(id),
              strategy TEXT NOT NULL,
              window_days INTEGER,
              policy_name TEXT NOT NULL,
              locked_at TEXT NOT NULL,
              locked_by TEXT NOT NULL DEFAULT 'admin',
              note TEXT
            );
            CREATE TABLE IF NOT EXISTS partitions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              partition_key TEXT NOT NULL,
              part_date TEXT,
              row_count INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT '写入中',
              in_window INTEGER NOT NULL DEFAULT 0,
              policy_name TEXT,
              writes_count INTEGER NOT NULL DEFAULT 0,
              last_writer TEXT,
              last_write_at TEXT,
              UNIQUE(pipeline_id, partition_key)
            );
            CREATE TABLE IF NOT EXISTS partition_writes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL,
              partition_id INTEGER,
              partition_key TEXT NOT NULL,
              writer TEXT NOT NULL,
              method TEXT NOT NULL,
              kind TEXT NOT NULL,
              event TEXT NOT NULL,
              status TEXT NOT NULL,
              reason TEXT,
              row_count INTEGER,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_open_write
              ON partition_writes(pipeline_id, partition_key)
              WHERE status = 'open';
            """
        )
        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            conn.executemany(
                "INSERT INTO pipelines (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEED_PIPELINES,
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
                (Config.LAB_NAME,),
            )


# ---------- 基础工具 ----------

def today_str() -> str:
    return date.today().isoformat()


def _parse_part_date(partition_key: str) -> date | None:
    try:
        return date.fromisoformat(partition_key.strip())
    except (ValueError, AttributeError):
        return None


def _in_window(part_date: date | None, policy: sqlite3.Row | dict | None, today: date) -> bool:
    if part_date is None or not policy:
        return False
    if policy["strategy"] == POLICY_CLEAR:
        # 先清空再写：回溯窗即全部历史
        return part_date <= today
    days = (today - part_date).days
    return 0 <= days < int(policy["window_days"] or 0)


def _policy_name(strategy: str, window_days: int | None) -> str:
    if strategy == POLICY_FIXED:
        return f"{POLICY_NAMES[POLICY_FIXED]}（{window_days}天）"
    return POLICY_NAMES[POLICY_CLEAR]


def _mock_rows(pipeline_id: int, writes_count: int) -> int:
    return 12 + (pipeline_id % 5) + (writes_count % 7)


# ---------- 管道（基线 + 总览分区统计） ----------

def _attach_summary(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    item = dict(row)
    stat = conn.execute(
        """
        SELECT COUNT(*) AS partition_count,
               COALESCE(SUM(CASE WHEN writes_count > 1 THEN 1 ELSE 0 END), 0) AS rewritten_count,
               COALESCE(SUM(CASE WHEN status = '写入中' THEN 1 ELSE 0 END), 0) AS writing_count,
               COALESCE(SUM(row_count), 0) AS total_rows
        FROM partitions WHERE pipeline_id = ?
        """,
        (item["id"],),
    ).fetchone()
    item.update(dict(stat))
    return item


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, loaded, created_at "
        "FROM pipelines ORDER BY id"
    ).fetchall()
    return [_attach_summary(conn, r) for r in rows]


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, loaded, created_at "
        "FROM pipelines WHERE id = ?",
        (pipeline_id,),
    ).fetchone()
    return _attach_summary(conn, row) if row else None


def run_extract(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if item["status"] not in ("待抽取", "抽取失败"):
        return item
    extracted = 12 + (pipeline_id % 5)
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0 WHERE id = ?",
        ("已抽取待加载", extracted, pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)


def run_load(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if item["status"] != "已抽取待加载":
        return item
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = extracted WHERE id = ?",
        ("已落库", pipeline_id),
    )
    return get_pipeline(conn, pipeline_id)


# ---------- 回溯策略（本日锁死） ----------

def get_policy(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM backfill_policies WHERE pipeline_id = ?",
        (pipeline_id,),
    ).fetchone()
    return dict(row) if row else None


def set_backfill_policy(
    conn: sqlite3.Connection,
    pipeline_id: int,
    strategy: str,
    window_days: int | None,
    user: str = "admin",
) -> dict:
    if not get_pipeline(conn, pipeline_id):
        raise PartitionError("管道不存在", status=404, code="not_found")
    if strategy not in (POLICY_FIXED, POLICY_CLEAR):
        raise PartitionError("回溯策略非法，只支持固定窗或先清空再写", 400, "bad_policy")
    if strategy == POLICY_FIXED:
        if window_days is None or int(window_days) <= 0:
            raise PartitionError("固定窗策略必须钉死一个正整数天数", 400, "bad_window")
        window_days = int(window_days)
    else:
        window_days = None
    existing = get_policy(conn, pipeline_id)
    today = today_str()
    if existing and existing["locked_at"] == today:
        raise PartitionError(
            f"本日回溯策略已锁死为「{existing['policy_name']}」，次日才能改",
            409,
            "policy_locked",
        )
    name = _policy_name(strategy, window_days)
    conn.execute(
        """
        INSERT INTO backfill_policies (pipeline_id, strategy, window_days, policy_name, locked_at, locked_by, note)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(pipeline_id) DO UPDATE SET
          strategy=excluded.strategy,
          window_days=excluded.window_days,
          policy_name=excluded.policy_name,
          locked_at=excluded.locked_at,
          locked_by=excluded.locked_by,
          note=excluded.note
        """,
        (
            pipeline_id,
            strategy,
            window_days,
            name,
            today,
            user,
            "本日锁死：只允许按该策略重落分区",
        ),
    )
    return get_policy(conn, pipeline_id)


# ---------- 分区占用板与详情 ----------

def _get_partition(conn: sqlite3.Connection, pipeline_id: int, partition_key: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM partitions WHERE pipeline_id = ? AND partition_key = ?",
        (pipeline_id, partition_key),
    ).fetchone()
    return dict(row) if row else None


def _get_open_write(conn: sqlite3.Connection, pipeline_id: int, partition_key: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM partition_writes WHERE pipeline_id = ? AND partition_key = ? AND status = 'open'",
        (pipeline_id, partition_key),
    ).fetchone()
    return dict(row) if row else None


def _has_open_today_write(conn: sqlite3.Connection, pipeline_id: int, today: date) -> bool:
    rows = conn.execute(
        "SELECT partition_key FROM partition_writes WHERE pipeline_id = ? AND status = 'open'",
        (pipeline_id,),
    ).fetchall()
    for r in rows:
        if _parse_part_date(r["partition_key"]) == today:
            return True
    return False


def _record_event(
    conn: sqlite3.Connection,
    pipeline_id: int,
    partition_id: int | None,
    partition_key: str,
    writer: str,
    method: str,
    kind: str,
    event: str,
    status: str,
    reason: str | None = None,
    row_count: int | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO partition_writes
          (pipeline_id, partition_id, partition_key, writer, method, kind, event, status, reason, row_count)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (pipeline_id, partition_id, partition_key, writer, method, kind, event, status, reason, row_count),
    )
    return cur.lastrowid


def _record_rejection(
    conn: sqlite3.Connection,
    pipeline_id: int,
    partition_id: int | None,
    partition_key: str,
    writer: str,
    method: str,
    kind: str,
    event: str,
    reason: str,
) -> None:
    """拒绝/抢占事件：写入后立即提交，保证随后抛错回滚时记录仍在。"""
    conn.execute(
        """
        INSERT INTO partition_writes
          (pipeline_id, partition_id, partition_key, writer, method, kind, event, status, reason, row_count)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (pipeline_id, partition_id, partition_key, writer, method, kind, event, "rejected", reason, None),
    )
    conn.commit()


def list_partitions(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    policy = get_policy(conn, pipeline_id)
    today = date.today()
    rows = conn.execute(
        "SELECT * FROM partitions WHERE pipeline_id = ? ORDER BY partition_key DESC, id DESC",
        (pipeline_id,),
    ).fetchall()
    items = []
    for r in rows:
        part = dict(r)
        live_in_window = _in_window(_parse_part_date(part["partition_key"]), policy, today)
        part["in_window"] = 1 if live_in_window else 0
        part["rewritten"] = 1 if part["writes_count"] > 1 else 0
        items.append(part)
    return items


def list_events(conn: sqlite3.Connection, pipeline_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM partition_writes WHERE pipeline_id = ? ORDER BY id DESC",
        (pipeline_id,),
    ).fetchall()
    out = []
    for r in rows:
        e = dict(r)
        e["method_label"] = METHOD_LABELS.get(e["method"], e["method"])
        out.append(e)
    return out


def get_partition_detail(conn: sqlite3.Connection, pipeline_id: int, partition_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM partitions WHERE id = ? AND pipeline_id = ?",
        (partition_id, pipeline_id),
    ).fetchone()
    if not row:
        return None
    part = dict(row)
    events = conn.execute(
        "SELECT * FROM partition_writes WHERE partition_id = ? ORDER BY id DESC",
        (partition_id,),
    ).fetchall()
    part["events"] = [dict(e) for e in events]
    return part


def backfill_overview(conn: sqlite3.Connection, pipeline_id: int) -> dict:
    if not get_pipeline(conn, pipeline_id):
        raise PartitionError("管道不存在", status=404, code="not_found")
    policy = get_policy(conn, pipeline_id)
    return {
        "today": today_str(),
        "policy": policy,
        "policy_locked_today": bool(policy and policy["locked_at"] == today_str()),
        "partitions": list_partitions(conn, pipeline_id),
        "events": list_events(conn, pipeline_id),
    }


# ---------- 分区写入：服务端裁决 ----------

def begin_partition_write(
    conn: sqlite3.Connection,
    pipeline_id: int,
    partition_key: str,
    method: str,
    writer: str,
) -> dict:
    key = (partition_key or "").strip()
    if not key:
        # 空分区键：直接失败，不留占用
        raise PartitionError("分区键不能为空，落库被拒绝", 400, "empty_key")
    if not get_pipeline(conn, pipeline_id):
        raise PartitionError("管道不存在", 404, "not_found")
    method = method if method in (METHOD_DIRECT, METHOD_CLEAR) else METHOD_DIRECT
    writer = (writer or "admin").strip() or "admin"
    today = date.today()

    part = _get_partition(conn, pipeline_id, key)
    occupied = bool(part and part["writes_count"] > 0)

    # 同一分区两人同刻落库：只成一笔，后来者抢占失败
    open_w = _get_open_write(conn, pipeline_id, key)
    if open_w:
        _record_rejection(
            conn, pipeline_id, part["id"] if part else None, key, writer,
            method, "backfill" if occupied else "first",
            EV_PREEMPT,
            reason=f"分区正在被 {open_w['writer']} 写入，抢占未成功，同刻只成一笔",
        )
        raise PartitionError(
            f"分区 {key} 正由 {open_w['writer']} 写入，抢占未成功，同一分区只成一笔",
            409,
            "contended",
        )

    # 今日分区正在写时，禁止开启回溯（重落）
    if occupied and _has_open_today_write(conn, pipeline_id, today):
        _record_rejection(
            conn, pipeline_id, part["id"], key, writer, method, "backfill",
            EV_REJECT, "今日分区正在写入，禁止开启回溯",
        )
        raise PartitionError("今日分区正在写入，禁止开启回溯", 409, "backfill_locked")

    if part is None:
        cur = conn.execute(
            "INSERT INTO partitions (pipeline_id, partition_key, part_date, status) VALUES (?,?,?,?)",
            (pipeline_id, key, _parse_part_date(key).isoformat() if _parse_part_date(key) else None, "写入中"),
        )
        part = _get_partition(conn, pipeline_id, key)
        part_id = part["id"]
    else:
        part_id = part["id"]
        conn.execute("UPDATE partitions SET status = '写入中' WHERE id = ?", (part_id,))

    kind = "backfill" if occupied else "first"
    try:
        write_id = _record_event(
            conn, pipeline_id, part_id, key, writer, method, kind, EV_OPEN, "open",
        )
    except sqlite3.IntegrityError:
        # 真并发兜底：部分唯一索引 idx_open_write 撞约束，说明同分区已有一笔开放写入
        conn.rollback()
        winner = _get_open_write(conn, pipeline_id, key)
        fresh = _get_partition(conn, pipeline_id, key)
        _record_rejection(
            conn, pipeline_id, fresh["id"] if fresh else None, key, writer,
            method, kind, EV_PREEMPT,
            reason=(
                f"分区正在被 {winner['writer'] if winner else '他人'} 写入，抢占未成功，同刻只成一笔"
            ),
        )
        raise PartitionError(
            f"分区 {key} 同时落库冲突，同一分区只成一笔",
            409,
            "contended",
        )
    return {
        "write_id": write_id,
        "partition_id": part_id,
        "partition_key": key,
        "kind": kind,
        "method": method,
        "writer": writer,
        "event": EV_OPEN,
    }


def _finish_with_rejection(conn: sqlite3.Connection, write_row: dict, part_id: int, reason: str) -> None:
    # 拒绝原因与占用板复位在同一事务内提交固化：随后抛 PartitionError，
    # get_db 的回滚不会抹掉这些已提交内容。
    conn.execute(
        "UPDATE partition_writes SET status = 'rejected', event = ?, reason = ? WHERE id = ?",
        (EV_REJECT, reason, write_row["id"]),
    )
    conn.execute(
        "UPDATE partitions SET status = '空闲' WHERE id = ? AND writes_count > 0",
        (part_id,),
    )
    # 从未成功落过库的分区，回到无占用
    conn.execute("DELETE FROM partitions WHERE id = ? AND writes_count = 0", (part_id,))
    conn.commit()


def commit_partition_write(
    conn: sqlite3.Connection, write_id: int, row_count: int | None = None
) -> dict:
    w = conn.execute("SELECT * FROM partition_writes WHERE id = ?", (write_id,)).fetchone()
    if not w:
        raise PartitionError("写入单不存在", 404, "not_found")
    w = dict(w)
    if w["status"] != "open":
        raise PartitionError(f"写入单已结束（{w['event']}），不能重复提交", 409, "write_closed")

    part = _get_partition(conn, w["pipeline_id"], w["partition_key"])
    part_id = w["partition_id"]
    today = date.today()

    if w["kind"] == "first":
        rows = int(row_count) if row_count is not None else _mock_rows(w["pipeline_id"], 1)
        policy = get_policy(conn, w["pipeline_id"])
        p_date = _parse_part_date(w["partition_key"])
        in_win = _in_window(p_date, policy, today)
        conn.execute(
            """
            UPDATE partitions SET status='空闲', row_count=?, writes_count=1, in_window=?,
              policy_name=?, last_writer=?, last_write_at=datetime('now') WHERE id=?
            """,
            (rows, 1 if in_win else 0, policy["policy_name"] if policy else None, w["writer"], part_id),
        )
        conn.execute(
            "UPDATE partition_writes SET status='success', event=?, row_count=? WHERE id=?",
            (EV_FIRST, rows, write_id),
        )
    else:
        # 重落（回溯）：本日必须已锁定策略
        policy = get_policy(conn, w["pipeline_id"])
        if not policy or policy["locked_at"] != today_str():
            reason = "本日回溯策略尚未锁定，禁止重落历史分区"
            _finish_with_rejection(conn, w, part_id, reason)
            raise PartitionError(reason, 409, "policy_not_locked")

        # 回溯法漂移：手法与策略对不上 → 回溯策略写歪了
        if policy["strategy"] == POLICY_FIXED and w["method"] == METHOD_CLEAR:
            reason = f"回溯策略写歪了：当前为「{policy['policy_name']}」，不允许清空再写"
            _finish_with_rejection(conn, w, part_id, reason)
            raise PartitionError(reason, 409, "drift")
        if policy["strategy"] == POLICY_CLEAR and w["method"] == METHOD_DIRECT:
            reason = f"回溯策略写歪了：当前为「{policy['policy_name']}」，必须整分区先清空再写"
            _finish_with_rejection(conn, w, part_id, reason)
            raise PartitionError(reason, 409, "drift")

        p_date = _parse_part_date(w["partition_key"])
        if policy["strategy"] == POLICY_FIXED and not _in_window(p_date, policy, today):
            reason = (
                f"分区 {w['partition_key']} 在回溯窗外（窗内仅最近 {policy['window_days']} 天），禁止重落"
            )
            _finish_with_rejection(conn, w, part_id, reason)
            raise PartitionError(reason, 409, "out_of_window")

        new_count = int(row_count) if row_count is not None else _mock_rows(
            w["pipeline_id"], part["writes_count"] + 1
        )

        if policy["strategy"] == POLICY_CLEAR:
            # 整分区先清空，再写
            _record_event(
                conn, w["pipeline_id"], part_id, w["partition_key"], w["writer"],
                w["method"], "backfill", EV_CLEAR, "success",
                reason=f"按「{policy['policy_name']}」整分区清空 {part['row_count']} 行后重写",
                row_count=0,
            )
            conn.execute("UPDATE partitions SET row_count = 0 WHERE id = ?", (part_id,))

        in_win = _in_window(p_date, policy, today)
        conn.execute(
            """
            UPDATE partitions SET status='空闲', row_count=?, writes_count=writes_count+1,
              in_window=?, policy_name=?, last_writer=?, last_write_at=datetime('now') WHERE id=?
            """,
            (new_count, 1 if in_win else 0, policy["policy_name"], w["writer"], part_id),
        )
        conn.execute(
            "UPDATE partition_writes SET status='success', event=?, row_count=? WHERE id=?",
            (EV_REWRITE, new_count, write_id),
        )

    return {
        "write_id": write_id,
        "partition": _get_partition(conn, w["pipeline_id"], w["partition_key"]),
        "event": EV_FIRST if w["kind"] == "first" else EV_REWRITE,
    }


def rollback_partition_write(conn: sqlite3.Connection, write_id: int) -> dict:
    w = conn.execute("SELECT * FROM partition_writes WHERE id = ?", (write_id,)).fetchone()
    if not w:
        raise PartitionError("写入单不存在", 404, "not_found")
    w = dict(w)
    if w["status"] != "open":
        raise PartitionError(f"写入单已结束（{w['event']}），不能回滚", 409, "write_closed")
    conn.execute(
        "UPDATE partition_writes SET status='rolled_back', event=?, reason='主动回滚，未落任何数据' WHERE id=?",
        (EV_ROLLBACK, write_id),
    )
    conn.execute(
        "UPDATE partitions SET status='空闲' WHERE id = ? AND writes_count > 0",
        (w["partition_id"],),
    )
    conn.execute(
        "DELETE FROM partitions WHERE id = ? AND writes_count = 0",
        (w["partition_id"],),
    )
    return {"write_id": write_id, "event": EV_ROLLBACK}


def write_partition(
    conn: sqlite3.Connection,
    pipeline_id: int,
    partition_key: str,
    method: str,
    writer: str,
    row_count: int | None = None,
) -> dict:
    """一键落库：begin + commit，裁决失败时不留开放写入单。"""
    begun = begin_partition_write(conn, pipeline_id, partition_key, method, writer)
    try:
        return commit_partition_write(conn, begun["write_id"], row_count)
    except PartitionError:
        # commit 内部已把分区复位、写入单标记拒绝
        raise
