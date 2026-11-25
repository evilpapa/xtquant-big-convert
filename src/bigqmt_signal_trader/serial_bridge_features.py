"""Snapshot refresh and download jobs backed by VK for the external serial bridge."""

import datetime
import math
import re
import time
import uuid

from .code_utils import normalize_stock_code
from .download_jobs import (
    DEFAULT_CHUNK_SIZE, DEFAULT_JOB_TTL_SECONDS, DONE, FAILED, PENDING, RUNNING,
    _dec, _enc, _new_download_job, _write_job, current_key, queue_key, read_download_status,
)
from .full_tick_cache import (
    MARKET_CODES, full_tick_request_id, iter_active_full_tick_demands, write_full_tick_cache,
)
from .vk_rpc import READ_METHODS


DOWNLOAD_METHODS = READ_METHODS & {
    "submit_download_history_data", "submit_download_history_data2",
    "get_download_status", "wait_download",
}
_TERMINAL_STATES = {DONE, FAILED}
_DOWNLOAD_TIMEOUT = 180
_VERIFY_WINDOW = 15
_VERIFY_INTERVAL = 0.5
_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))
_JOB_ID = re.compile(r"[0-9a-f]{16}\Z")


def _history_time(value, end=False):
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value or "").strip()
    if text.isdecimal() and len(text) in (10, 13):
        seconds = int(text) / (1000 if len(text) == 13 else 1)
        return datetime.datetime.fromtimestamp(seconds, _SHANGHAI).replace(tzinfo=None)
    if text.isdecimal() and len(text) in (8, 14):
        result = datetime.datetime.strptime(text, "%Y%m%d" if len(text) == 8 else "%Y%m%d%H%M%S")
    else:
        result = datetime.datetime.fromisoformat(text)
        if result.tzinfo is not None:
            result = result.astimezone(_SHANGHAI).replace(tzinfo=None)
    if end and len(text) in (8, 10) and (len(text) == 8 or not text.isdecimal()):
        result = datetime.datetime.combine(result.date(), datetime.time.max)
    return result


def _history_rows(frame):
    if isinstance(frame, dict) and frame.get("__bigqmt_type__") == "DataFrame":
        frame = frame.get("records")
    if isinstance(frame, list):
        return (row for row in frame if isinstance(row, dict))
    if isinstance(frame, dict):
        arrays = {key: values for key, values in frame.items() if isinstance(values, list)}
        if arrays:
            return (dict(zip(arrays, row)) for row in zip(*arrays.values()))
        return iter((frame,))
    return iter(())


def _has_history(frame, price_field, start, end):
    for row in _history_rows(frame):
        try:
            price = float(row.get(price_field))
        except (ValueError, TypeError, OverflowError):
            continue
        if not math.isfinite(price) or price == 0:
            continue
        for field in ("stime", "time", "date", "datetime", "timetag", "index"):
            try:
                stamp = _history_time(row.get(field))
            except (ValueError, OverflowError, OSError):
                continue
            if stamp.year >= 1990 and (start is None or stamp >= start) and (end is None or stamp <= end):
                return True
            break
    return False


class SerialBridgeFeatures:
    def __init__(self, vk_client, account_id):
        self.vk = vk_client
        self.account_id = account_id
        self.active_key = current_key(account_id) + ":serial"
        self._waiters = {}
        self._snapshot_attempts = {}
        self._next_snapshot_scan = 0.0
        self._next_download_poll = 0.0
        self._prefer_download = False

    def _submit(self, method, params, request_id):
        job_id = uuid.uuid5(uuid.NAMESPACE_OID, request_id).hex[:16]
        existing = read_download_status(self.vk, self.account_id, job_id)
        if existing is not None:
            return existing
        codes = (params.get("stock_code") or params.get("code")
                 if method == "submit_download_history_data"
                 else params.get("stock_list") or params.get("stock_code"))
        if isinstance(codes, str):
            codes = [codes]
        if not isinstance(codes, list) or not codes or not all(isinstance(code, str) for code in codes):
            raise ValueError("stock_list or stock_code is required")
        codes = [normalize_stock_code(code) for code in codes]
        start = str(params.get("start_time") or "")
        end = str(params.get("end_time") or "")
        first = _history_time(start) if start else None
        last = _history_time(end, end=True) if end else None
        if first is not None and last is not None and first > last:
            raise ValueError("start_time must be before or equal to end_time")
        chunk_size = max(1, min(DEFAULT_CHUNK_SIZE, int(params.get("chunk_size") or DEFAULT_CHUNK_SIZE)))
        ttl = max(1, min(86400, int(params.get("job_ttl_seconds") or DEFAULT_JOB_TTL_SECONDS)))
        job = _new_download_job(
            job_id, codes, str(params.get("period") or "1d"), start_time=start, end_time=end,
            incrementally=params.get("incrementally"), chunk_size=chunk_size,
        )
        job.update(accepted=0, job_ttl_seconds=ttl)
        with self.vk.pipeline(transaction=True) as pipe:
            _write_job(pipe, self.account_id, job, ttl)
            pipe.rpush(queue_key(self.account_id), _enc(job_id))
            pipe.execute()
        return job

    def handle_download(self, method, params, request_id, timeout, reply):
        if method.startswith("submit_"):
            reply(self._submit(method, params, request_id))
            return
        job_id = params.get("job_id")
        if not isinstance(job_id, str) or not _JOB_ID.fullmatch(job_id):
            raise ValueError("a valid job_id is required")
        status = read_download_status(self.vk, self.account_id, job_id)
        if method == "get_download_status":
            if status is None:
                raise KeyError("download job not found or expired: %s" % job_id)
            reply(status)
            return
        wait = float(params.get("wait_seconds", 600))
        interval = float(params.get("poll_interval_seconds", 0.5))
        if not math.isfinite(wait) or not math.isfinite(interval):
            raise ValueError("invalid download wait timing")
        if wait <= 0 or status is None or status["state"] in _TERMINAL_STATES:
            reply(status)
            return
        if len(self._waiters) >= 128 and request_id not in self._waiters:
            raise ValueError("too many pending download waits")
        now = time.monotonic()
        deadline = now + min(wait, max(0, timeout - 0.05))
        self._waiters[request_id] = (job_id, deadline, now, max(0.05, interval), reply)

    def poll_waiters(self, now):
        statuses = {}
        for request_id, (job_id, deadline, next_poll, interval, reply) in list(self._waiters.items()):
            if now < min(deadline, next_poll):
                continue
            if job_id not in statuses:
                statuses[job_id] = read_download_status(self.vk, self.account_id, job_id)
            status = statuses[job_id]
            if now >= deadline or status is None or status["state"] in _TERMINAL_STATES:
                del self._waiters[request_id]
                reply(status)
            else:
                self._waiters[request_id] = (job_id, deadline, now + interval, interval, reply)

    def _refresh_snapshot(self, now, send):
        if now < self._next_snapshot_scan:
            return False
        self._next_snapshot_scan = now + 0.2
        demands = iter_active_full_tick_demands(self.vk, self.account_id)
        candidates = {}
        for demand in demands:
            codes = demand["codes"]
            key = full_tick_request_id(codes)
            is_market = not MARKET_CODES.isdisjoint(codes)
            candidates[key] = (demand, is_market)
        self._snapshot_attempts = {
            key: last for key, last in self._snapshot_attempts.items() if key in candidates
        }
        for key in sorted(candidates, key=lambda key: self._snapshot_attempts.get(key, 0)):
            demand, is_market = candidates[key]
            if now - self._snapshot_attempts.get(key, -math.inf) < (3 if is_market else 0.5):
                continue
            self._snapshot_attempts[key] = now

            def cache_response(response):
                data = response.get("data")
                if (response.get("ok") and isinstance(data, dict) and data
                        and demand["expires_at_ts"] > time.time()):
                    write_full_tick_cache(
                        self.vk, self.account_id, demand["codes"], data,
                        cache_ttl_seconds=min(60, demand["cache_ttl_seconds"]),
                    )

            send("get_full_tick", {"codes": demand["codes"]}, 30 if is_market else 8, cache_response)
            return True
        return False

    def _save_job(self, job):
        ttl = job.get("job_ttl_seconds", DEFAULT_JOB_TTL_SECONDS)
        if job["state"] == RUNNING:
            ttl = max(ttl, _DOWNLOAD_TIMEOUT + _VERIFY_WINDOW + 5)
        _write_job(self.vk, self.account_id, job, ttl)
        if job["state"] in _TERMINAL_STATES:
            self.vk.lpop(self.active_key)

    def _fail_job(self, job, error):
        job.update(state=FAILED, error=error)
        self._save_job(job)

    def _download_step(self, now, send):
        if now < self._next_download_poll:
            return False
        self._next_download_poll = now + 0.1
        token = self.vk.lindex(self.active_key, 0)
        if token is None:
            token = self.vk.lmove(queue_key(self.account_id), self.active_key, "LEFT", "RIGHT")
        if token is None:
            return False
        job = read_download_status(self.vk, self.account_id, _dec(token))
        if not job or job["state"] not in (PENDING, RUNNING):
            self.vk.lpop(self.active_key)
            return False
        if job.get("phase") == "submitting":
            self._fail_job(job, "bridge restarted before QMT confirmed the batch; submission outcome unknown")
            return False
        done = job["done"]
        codes = job["stock_list"][done:done + job["chunk_size"]]
        params = {name: job[name] for name in ("period", "start_time", "end_time")}
        params["stock_list"] = codes
        if job.get("phase") != "verifying":
            job.update(state=RUNNING, phase="submitting")
            self._save_job(job)

            def accepted(response):
                if not response.get("ok") or not response.get("data"):
                    self._fail_job(job, response.get("error") or "QMT rejected the download batch")
                    return
                job.update(phase="verifying", accepted=done + len(codes), verify_until_ts=None)
                self._save_job(job)

            send("download_history_data2", dict(params, incrementally=job.get("incrementally")),
                 _DOWNLOAD_TIMEOUT, accepted)
            return True

        verify_until = job.get("verify_until_ts")
        if verify_until is None:
            verify_until = job["verify_until_ts"] = time.time() + _VERIFY_WINDOW
            self._save_job(job)
        elif time.time() >= verify_until:
            self._fail_job(job, "QMT accepted the batch but history was not readable: " + ", ".join(codes))
            return False
        self._next_download_poll = now + _VERIFY_INTERVAL
        price_field = "lastPrice" if str(job["period"]).lower() == "tick" else "close"
        start = _history_time(job["start_time"]) if job["start_time"] else None
        end = _history_time(job["end_time"], end=True) if job["end_time"] else None

        def verified(response):
            data = response.get("data")
            if not response.get("ok") or not isinstance(data, dict):
                if time.time() >= verify_until:
                    self._fail_job(job, response.get("error") or "QMT history readback failed")
                return
            if data.get("__bigqmt_type__") == "Panel":
                data = data.get("data") or {}
            elif data.get("__bigqmt_type__") == "DataFrame" and len(codes) == 1:
                data = {codes[0]: data}
            frames = {code.upper(): frame for code, frame in data.items()}
            missing = [
                code for code in codes
                if not _has_history(frames.get(code.upper()), price_field, start, end)
            ]
            if not missing:
                job["done"] += len(codes)
                job.update(state=DONE if job["done"] >= job["total"] else RUNNING,
                           phase="verified" if job["done"] >= job["total"] else "queued")
                self._save_job(job)
            elif time.time() >= verify_until:
                self._fail_job(job, "QMT accepted the batch but history was not readable: " + ", ".join(missing))

        send("get_local_data", dict(params, field_list=[price_field],
                                    count=1, fill_data=False, subscribe=False), 5, verified)
        return True

    def step(self, now, send):
        first, second = ((self._download_step, self._refresh_snapshot) if self._prefer_download
                         else (self._refresh_snapshot, self._download_step))
        if first(now, send):
            self._prefer_download = not self._prefer_download
            return True
        return second(now, send)
