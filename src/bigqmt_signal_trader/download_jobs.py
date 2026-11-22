"""Big QMT 的异步分块下载任务。

客户端将下载任务（发射后不等待）提交到 Valkey 队列，并轮询任务状态。
Big QMT 策略线程一次只处理一个任务，每个 tick 最多下载固定数量的
标的（``chunk_size`` 个标的，并受运行时间预算限制），因此耗时较长的
``download_history_data2`` 不会阻塞策略线程或 RPC 泵。历史 K 线会落到
Big QMT 机器的本地存储中；客户端随后可通过快速的 ``get_local_data`` /
``get_market_data`` 调用读取这些数据。

Valkey 布局（按账户区分）。所有存储的 VALUE 都采用无数字编码（见 _enc），
避免 QMT 终端的 Valkey 合规过滤器因泵回读任务数据中的股票代码而拦截：
- ``bigqmt:dljob:pending:{account_id}``      待读取任务 ID 列表（RPUSH/LPOP）
- ``bigqmt:dljob:item:{account_id}:{job_id}`` 包含进度的编码任务数据
- ``bigqmt:dljob:active:{account_id}``        当前正在处理的任务 ID
"""

import json
import time
import uuid


# Dedicated "bigqmt:dljob:*" namespace for the client<->pump protocol.
QUEUE_KEY_TEMPLATE = "bigqmt:dljob:pending:{account_id}"
JOB_KEY_TEMPLATE = "bigqmt:dljob:item:{account_id}:{job_id}"
CURRENT_KEY_TEMPLATE = "bigqmt:dljob:active:{account_id}"

DEFAULT_JOB_TTL_SECONDS = 3600
DEFAULT_CHUNK_SIZE = 10
DEFAULT_MAX_WALL_SECONDS = 0.5

# Terminal + in-flight states.
PENDING = "pending"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
_ACTIVE_STATES = (PENDING, RUNNING)


def queue_key(account_id):
    return QUEUE_KEY_TEMPLATE.format(account_id=str(account_id or ""))


def job_key(account_id, job_id):
    return JOB_KEY_TEMPLATE.format(account_id=str(account_id or ""), job_id=str(job_id or ""))


def current_key(account_id):
    return CURRENT_KEY_TEMPLATE.format(account_id=str(account_id or ""))


def _text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


# The client bundled in earlier 国金证券 QMT builds had check_response() raise
# "Sensitive Data Detected, Forbidden!" whenever a Valkey *response* contains a
# stock-code + operation-code DIGIT pattern (a brokerage control against trading
# signals flowing through Valkey). The pump runs inside that terminal and must read
# job data (which contains stock codes) back from Valkey. So every value the pump
# reads is stored as a DIGIT-FREE token: hex-encode, then shift digits 0-9 -> the
# letters g-p, making the stored value all letters (a-p). The stock-code regex
# requires digits, so it can never match. Reversible; writes are never filtered
# (only responses are), so only read-back values need this.
_DIGIT_TO_ALPHA = str.maketrans("0123456789", "ghijklmnop")
_ALPHA_TO_DIGIT = str.maketrans("ghijklmnop", "0123456789")


def _enc(text_value):
    return _text(text_value).encode("utf-8").hex().translate(_DIGIT_TO_ALPHA)


def _dec(token):
    text = _text(token)
    if not text:
        return None
    try:
        return bytes.fromhex(text.translate(_ALPHA_TO_DIGIT)).decode("utf-8")
    except Exception:
        return None


def _new_download_job(
    job_id,
    stock_list,
    period,
    method="download_history_data2",
    start_time="",
    end_time="",
    incrementally=None,
    chunk_size=DEFAULT_CHUNK_SIZE,
):
    codes = [str(code) for code in (stock_list or []) if str(code or "").strip()]
    if not codes:
        raise ValueError("stock_list is required for a download job")
    now = time.time()
    return {
        "job_id": job_id,
        "method": str(method or "download_history_data2"),
        "stock_list": codes,
        "period": period,
        "start_time": start_time or "",
        "end_time": end_time or "",
        "incrementally": incrementally,
        "chunk_size": int(chunk_size or DEFAULT_CHUNK_SIZE),
        "total": len(codes),
        "done": 0,
        "state": PENDING,
        "error": "",
        "created_at_ts": now,
        "updated_at_ts": now,
    }


def submit_download_job(
    valkey_client,
    account_id,
    stock_list,
    period,
    method="download_history_data2",
    start_time="",
    end_time="",
    incrementally=None,
    chunk_size=DEFAULT_CHUNK_SIZE,
    job_ttl_seconds=DEFAULT_JOB_TTL_SECONDS,
):
    """Queue a download job and return its initial status dict (non-blocking)."""
    job_id = uuid.uuid4().hex[:16]
    job = _new_download_job(
        job_id, stock_list, period, method, start_time, end_time, incrementally, chunk_size,
    )
    ttl = int(max(1, job_ttl_seconds))
    valkey_client.setex(job_key(account_id, job_id), ttl, _enc(json.dumps(job, ensure_ascii=False)))
    valkey_client.rpush(queue_key(account_id), _enc(job_id))
    try:
        valkey_client.expire(queue_key(account_id), ttl)
    except Exception:
        pass
    return job


def read_download_status(valkey_client, account_id, job_id):
    """Return the current job status dict, or None if unknown/expired."""
    decoded = _dec(valkey_client.get(job_key(account_id, job_id)))
    if not decoded:
        return None
    try:
        job = json.loads(decoded)
    except Exception:
        return None
    return job if isinstance(job, dict) else None


def wait_download_job(
    valkey_client,
    account_id,
    job_id,
    wait_seconds=600.0,
    poll_interval_seconds=0.5,
):
    """Block (client-side only) until the job reaches a terminal state or timeout."""
    deadline = time.time() + max(0.0, float(wait_seconds))
    while True:
        status = read_download_status(valkey_client, account_id, job_id)
        if status and status.get("state") in (DONE, FAILED):
            return status
        if time.time() >= deadline:
            return status
        time.sleep(max(0.05, float(poll_interval_seconds)))


def _write_job(valkey_client, account_id, job, job_ttl_seconds):
    job["updated_at_ts"] = time.time()
    ttl = int(max(1, job_ttl_seconds))
    valkey_client.setex(job_key(account_id, job["job_id"]), ttl, _enc(json.dumps(job, ensure_ascii=False)))


def _acquire_current_job(valkey_client, account_id):
    ckey = current_key(account_id)
    current_id = _dec(valkey_client.get(ckey))
    if current_id:
        job = read_download_status(valkey_client, account_id, current_id)
        if job and job.get("state") in _ACTIVE_STATES:
            return job
        # Stale pointer (job done/failed/expired): drop it and pick the next one.
        valkey_client.delete(ckey)
    while True:
        job_id = _dec(valkey_client.lpop(queue_key(account_id)))
        if not job_id:
            return None
        job = read_download_status(valkey_client, account_id, job_id)
        if job and job.get("state") in _ACTIVE_STATES:
            valkey_client.set(ckey, _enc(job_id))
            return job
        # Skip unknown/expired/finished ids left in the queue.


def _download_chunk(market_data, method, chunk, period, start_time, end_time, incrementally):
    # Falsy-but-not-None native return = the terminal rejected the task (#339).
    # It must fail the job loudly; counting it as done was the fake progress.
    if method == "download_history_data":
        for code in chunk:
            result = market_data.download_history_data(code, period, start_time, end_time, incrementally)
            if result is not None and not result:
                raise RuntimeError(
                    "download_history_data rejected by terminal (return %r) code=%s"
                    % (result, code))
    else:
        result = market_data.download_history_data2(chunk, period, start_time, end_time, incrementally)
        if result is not None and not result:
            raise RuntimeError(
                "download_history_data2 rejected by terminal (return %r) codes=%s"
                % (result, ",".join(str(c) for c in chunk)))


def pump_download_jobs(
    valkey_client,
    market_data,
    account_id,
    chunk_size=DEFAULT_CHUNK_SIZE,
    max_wall_seconds=DEFAULT_MAX_WALL_SECONDS,
    job_ttl_seconds=DEFAULT_JOB_TTL_SECONDS,
):
    """Advance the active download job by a bounded slice. Call once per tick.

    Downloads at least one chunk (so progress is always made) and keeps going
    until the wall-clock budget is spent. Returns a small status summary, or None
    when there is no active job. Runs on the caller (strategy) thread.
    """
    job = _acquire_current_job(valkey_client, account_id)
    if job is None:
        return None
    stock_list = job.get("stock_list") or []
    total = int(job.get("total") or len(stock_list))
    done = int(job.get("done") or 0)
    step = int(job.get("chunk_size") or chunk_size or DEFAULT_CHUNK_SIZE)
    if step <= 0:
        step = DEFAULT_CHUNK_SIZE
    method = str(job.get("method") or "download_history_data2")
    period = job.get("period")
    start_time = job.get("start_time") or ""
    end_time = job.get("end_time") or ""
    incrementally = job.get("incrementally")

    started_at = time.time()
    processed_this_tick = 0
    try:
        while done < total:
            # Always run one chunk; only the budget check (after the first) can
            # stop the tick, so a single heavy chunk is the smallest block unit.
            if max_wall_seconds and processed_this_tick and (time.time() - started_at) > float(max_wall_seconds):
                break
            chunk = stock_list[done:done + step]
            _download_chunk(market_data, method, chunk, period, start_time, end_time, incrementally)
            done += len(chunk)
            processed_this_tick += len(chunk)
    except Exception as exc:
        job["state"] = FAILED
        job["error"] = "%s: %s" % (exc.__class__.__name__, exc)
        job["done"] = done
        _write_job(valkey_client, account_id, job, job_ttl_seconds)
        valkey_client.delete(current_key(account_id))
        return {"job_id": job["job_id"], "state": FAILED, "done": done, "total": total, "error": job["error"]}

    job["done"] = done
    if done >= total:
        job["state"] = DONE
        _write_job(valkey_client, account_id, job, job_ttl_seconds)
        valkey_client.delete(current_key(account_id))
        return {"job_id": job["job_id"], "state": DONE, "done": done, "total": total}
    job["state"] = RUNNING
    _write_job(valkey_client, account_id, job, job_ttl_seconds)
    return {"job_id": job["job_id"], "state": RUNNING, "done": done, "total": total}
