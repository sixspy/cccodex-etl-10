"""分区落库与回溯窗：窗内重落、清空历史、争抢、空键、写入中禁回溯、策略漂移等。"""
import sqlite3
from datetime import date, timedelta

import pytest

from app.db import (
    METHOD_CLEAR,
    METHOD_DIRECT,
    POLICY_CLEAR,
    POLICY_FIXED,
    connect,
    get_db,
)


def _pid(authed):
    return authed.get("/api/pipelines").json()["items"][0]["id"]


def _set_policy(authed, pid, strategy, window_days=None):
    body = {"strategy": strategy}
    if window_days is not None:
        body["window_days"] = window_days
    return authed.put(f"/api/pipelines/{pid}/backfill/policy", json=body)


def _write(authed, pid, key, method=METHOD_DIRECT, writer=None, row_count=None):
    body = {"partition_key": key, "method": method}
    if writer:
        body["writer"] = writer
    if row_count is not None:
        body["row_count"] = row_count
    return authed.post(f"/api/pipelines/{pid}/partitions/write", json=body)


def _overview(authed, pid):
    return authed.get(f"/api/pipelines/{pid}/backfill/overview").json()


def _today():
    return date.today().isoformat()


def _days_ago(n):
    return (date.today() - timedelta(days=n)).isoformat()


# ---------- 基础：空键失败、首次落库 ----------

def test_empty_partition_key_fails(authed):
    pid = _pid(authed)
    r = _write(authed, pid, "   ")
    assert r.status_code == 400
    assert "分区键不能为空" in r.json()["error"]
    # 不留任何分区占用
    assert _overview(authed, pid)["partitions"] == []


def test_first_write_creates_partition_on_board(authed):
    pid = _pid(authed)
    r = _write(authed, pid, _today(), row_count=20)
    assert r.status_code == 200
    assert r.json()["event"] == "落库成功"
    board = _overview(authed, pid)["partitions"]
    assert len(board) == 1
    assert board[0]["partition_key"] == _today()
    assert board[0]["row_count"] == 20
    assert board[0]["writes_count"] == 1
    # 占用板含分区键、行数、是否在窗内、策略名
    assert "in_window" in board[0]


# ---------- 固定窗：窗内重落 ----------

def test_fixed_window_in_window_rewrite(authed):
    pid = _pid(authed)
    _write(authed, pid, _days_ago(1), row_count=10)
    assert _set_policy(authed, pid, POLICY_FIXED, 3).status_code == 200
    r = _write(authed, pid, _days_ago(1), METHOD_DIRECT, row_count=33)
    assert r.status_code == 200
    assert r.json()["event"] == "重写成功"
    part = r.json()["partition"]
    assert part["row_count"] == 33
    assert part["writes_count"] == 2
    assert part["in_window"] == 1
    # 总览能看出分区已重写
    pipe = authed.get(f"/api/pipelines/{pid}").json()
    assert pipe["rewritten_count"] == 1
    board = _overview(authed, pid)["partitions"]
    assert board[0]["policy_name"].startswith("固定窗重落")


def test_fixed_window_out_of_window_rejected(authed):
    pid = _pid(authed)
    _write(authed, pid, _days_ago(9), row_count=10)
    _set_policy(authed, pid, POLICY_FIXED, 3)
    r = _write(authed, pid, _days_ago(9), METHOD_DIRECT)
    assert r.status_code == 409
    assert "回溯窗外" in r.json()["error"]
    # 原数据未被破坏
    board = _overview(authed, pid)["partitions"]
    assert board[0]["row_count"] == 10
    assert board[0]["writes_count"] == 1
    # 页面能分清窗外分区
    assert board[0]["in_window"] == 0


def test_fixed_window_clear_method_is_drift(authed):
    pid = _pid(authed)
    _write(authed, pid, _days_ago(1), row_count=10)
    _set_policy(authed, pid, POLICY_FIXED, 3)
    r = _write(authed, pid, _days_ago(1), METHOD_CLEAR)
    assert r.status_code == 409
    assert "回溯策略写歪了" in r.json()["error"]
    assert r.json()["code"] == "drift"


# ---------- 先清空再写：清空历史 ----------

def test_clear_first_policy_requires_clear_then_write(authed):
    pid = _pid(authed)
    old_key = _days_ago(30)
    _write(authed, pid, old_key, row_count=50)
    _set_policy(authed, pid, POLICY_CLEAR)
    # 直接重落 → 手法漂移
    drift = _write(authed, pid, old_key, METHOD_DIRECT)
    assert drift.status_code == 409
    assert "回溯策略写歪了" in drift.json()["error"]
    # 按策略清空再写 → 任意历史分区可重落，且在窗内标记为窗内（历史即窗）
    ok = _write(authed, pid, old_key, METHOD_CLEAR, row_count=77)
    assert ok.status_code == 200
    assert ok.json()["event"] == "重写成功"
    assert ok.json()["partition"]["row_count"] == 77
    assert ok.json()["partition"]["in_window"] == 1
    # 回溯详情能读到清空记录
    events = _overview(authed, pid)["events"]
    clears = [e for e in events if e["event"] == "清空"]
    assert clears and "清空 50 行" in clears[0]["reason"]


def test_clear_first_reject_reason_visible_in_detail(authed):
    pid = _pid(authed)
    _write(authed, pid, _days_ago(5), row_count=12)
    _set_policy(authed, pid, POLICY_CLEAR)
    r = _write(authed, pid, _days_ago(5), METHOD_DIRECT)
    assert r.status_code == 409
    ov = _overview(authed, pid)
    rejects = [e for e in ov["events"] if e["status"] == "rejected"]
    assert rejects and "回溯策略写歪了" in rejects[0]["reason"]
    # 分区详情接口同样可读
    part_id = ov["partitions"][0]["id"]
    detail = authed.get(f"/api/pipelines/{pid}/partitions/{part_id}").json()
    assert any(e["status"] == "rejected" for e in detail["events"])


# ---------- 策略本日锁死、天数钉死、次日可改 ----------

def test_policy_locked_same_day_and_unlocked_next_day(authed):
    pid = _pid(authed)
    assert _set_policy(authed, pid, POLICY_FIXED, 3).status_code == 200
    again = _set_policy(authed, pid, POLICY_CLEAR)
    assert again.status_code == 409
    assert "本日回溯策略已锁死" in again.json()["error"]
    # 天数必须是正整数
    bad = authed.put(
        f"/api/pipelines/{pid}/backfill/policy",
        json={"strategy": POLICY_FIXED, "window_days": 0},
    )
    assert bad.status_code == 400
    # 模拟次日：锁日期变为昨天，允许改策略
    with get_db() as conn:
        conn.execute(
            "UPDATE backfill_policies SET locked_at = ? WHERE pipeline_id = ?",
            (_days_ago(1), pid),
        )
    ok = _set_policy(authed, pid, POLICY_CLEAR)
    assert ok.status_code == 200
    assert ok.json()["policy"]["strategy"] == POLICY_CLEAR


def test_backfill_requires_locked_policy(authed):
    pid = _pid(authed)
    _write(authed, pid, _days_ago(2), row_count=10)
    # 未选策略直接重落
    r = _write(authed, pid, _days_ago(2), METHOD_DIRECT)
    assert r.status_code == 409
    assert "尚未锁定" in r.json()["error"]


# ---------- 同分区争抢：只成一笔 ----------

def test_concurrent_same_partition_only_one_wins(authed):
    pid = _pid(authed)
    key = _today()
    b1 = authed.post(
        f"/api/pipelines/{pid}/partitions/begin",
        json={"partition_key": key, "method": METHOD_DIRECT, "writer": "甲"},
    )
    assert b1.status_code == 200
    b2 = authed.post(
        f"/api/pipelines/{pid}/partitions/begin",
        json={"partition_key": key, "method": METHOD_DIRECT, "writer": "乙"},
    )
    assert b2.status_code == 409
    assert b2.json()["code"] == "contended"
    # 抢占记录落库可读
    events = _overview(authed, pid)["events"]
    pre = [e for e in events if e["event"] == "抢占失败"]
    assert pre and pre[0]["writer"] == "乙"
    assert "甲" in pre[0]["reason"]
    # 甲正常提交，只成一笔
    c = authed.post(f"/api/writes/{b1.json()['write_id']}/commit", json={"row_count": 15})
    assert c.status_code == 200
    board = _overview(authed, pid)["partitions"]
    assert len(board) == 1
    assert board[0]["row_count"] == 15
    assert board[0]["last_writer"] == "甲"


def test_db_unique_index_guards_real_concurrent_open(authed):
    # 直接绕过应用层检查，模拟两人同刻各自插入开放写入单：由部分唯一索引裁决
    from app.db import connect
    pid = _pid(authed)
    key = _today()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO partition_writes
              (pipeline_id, partition_id, partition_key, writer, method, kind, event, status)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (pid, None, key, "甲", METHOD_DIRECT, "first", "写入中", "open"),
        )
    with connect() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO partition_writes
                  (pipeline_id, partition_id, partition_key, writer, method, kind, event, status)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (pid, None, key, "乙", METHOD_DIRECT, "first", "写入中", "open"),
            )


def test_rollback_releases_partition_for_rewrite(authed):
    pid = _pid(authed)
    key = _today()
    b = authed.post(
        f"/api/pipelines/{pid}/partitions/begin",
        json={"partition_key": key, "method": METHOD_DIRECT, "writer": "甲"},
    ).json()
    assert authed.post(f"/api/writes/{b['write_id']}/rollback").status_code == 200
    # 回滚后无残留分区，别人可重新落库
    assert _overview(authed, pid)["partitions"] == []
    again = _write(authed, pid, key, METHOD_DIRECT, writer="乙", row_count=9)
    assert again.status_code == 200
    assert again.json()["partition"]["last_writer"] == "乙"


# ---------- 今日分区写入中禁回溯 ----------

def test_no_backfill_while_today_partition_writing(authed):
    pid = _pid(authed)
    _set_policy(authed, pid, POLICY_FIXED, 5)
    # 历史分区先落一笔，供重落
    _write(authed, pid, _days_ago(2), row_count=10)
    # 今日分区开始写但不提交
    open_today = authed.post(
        f"/api/pipelines/{pid}/partitions/begin",
        json={"partition_key": _today(), "method": METHOD_DIRECT, "writer": "甲"},
    )
    assert open_today.status_code == 200
    # 此时对历史分区开回溯 → 禁止
    r = authed.post(
        f"/api/pipelines/{pid}/partitions/begin",
        json={"partition_key": _days_ago(2), "method": METHOD_DIRECT, "writer": "乙"},
    )
    assert r.status_code == 409
    assert "今日分区正在写入" in r.json()["error"]
    # 提交今日写入后，回溯恢复
    authed.post(f"/api/writes/{open_today.json()['write_id']}/commit", json={"row_count": 8})
    ok = _write(authed, pid, _days_ago(2), METHOD_DIRECT, row_count=11)
    assert ok.status_code == 200
    assert ok.json()["event"] == "重写成功"


# ---------- 总览与占用板 ----------

def test_overview_distinguishes_in_and_out_of_window(authed):
    pid = _pid(authed)
    _write(authed, pid, _days_ago(1), row_count=5)
    _write(authed, pid, _days_ago(20), row_count=6)
    _set_policy(authed, pid, POLICY_FIXED, 3)
    board = _overview(authed, pid)["partitions"]
    by_key = {p["partition_key"]: p for p in board}
    assert by_key[_days_ago(1)]["in_window"] == 1
    assert by_key[_days_ago(20)]["in_window"] == 0
    ov = _overview(authed, pid)
    assert ov["policy_locked_today"] is True
    assert ov["today"] == _today()


def test_pipeline_overview_counts_partitions(authed):
    pid = _pid(authed)
    _set_policy(authed, pid, POLICY_FIXED, 5)
    _write(authed, pid, _days_ago(1), row_count=5)
    _write(authed, pid, _days_ago(1), METHOD_DIRECT, row_count=8)
    pipe = authed.get(f"/api/pipelines/{pid}").json()
    assert pipe["partition_count"] == 1
    assert pipe["rewritten_count"] == 1
    assert pipe["total_rows"] == 8
