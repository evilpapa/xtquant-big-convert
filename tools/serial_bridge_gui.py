# coding: utf-8
"""Big QMT 串口桥接控制台（tkinter GUI）。

启动 / 停止 / 重启 serial_vk_bridge 和大 QMT，并实时 tail 两路日志：

1. 桥接子进程的 stdout/stderr（``serial_vk_bridge 的启动日志``）；
2. ``<QMT>/userdata/log/XtClient_FormulaOutput_YYYYMMDD.log``（QMT 策略日志）。

源码运行：

    python tools/serial_bridge_gui.py

打包成单文件 exe：

    python tools/build_gui_exe.py
"""

import argparse
from contextlib import suppress
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

import serial_controller_core as core


LOG_LINE_LIMIT = 4000
BRIDGE_READY_MARK = "串口桥接已启动"
SESSION_READY_MARK = "QMT 串口会话已就绪"
BRIDGE_CHECK_INTERVAL_MS = 20_000


def classify_line(line):
    """给日志行选一个颜色标签。"""
    lower = line.lower()
    if ("error" in lower or "traceback" in lower or "无法" in line
            or "失败" in line):
        return "error"
    if ("warning" in lower or "warn" in lower or "未收到" in line
            or "已丢弃" in line or "异常" in line):
        return "warn"
    if "就绪" in line or "已启动" in line or "操作成功" in line or "已关闭" in line:
        return "ok"
    return "info"


class LogPane(ttk.Frame):
    """只读日志面板：自动滚动、行数上限、清空。"""

    def __init__(self, master):
        super().__init__(master)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        toolbar = ttk.Frame(self)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        self.autoscroll = tk.BooleanVar(value=True)
        ttk.Checkbutton(toolbar, text="自动滚动",
                        variable=self.autoscroll).pack(side="left")
        ttk.Button(toolbar, text="清空", width=6,
                   command=self.clear).pack(side="right")

        self.text = ScrolledText(self, wrap="none", height=20,
                                 font=("Microsoft YaHei UI", 9), state="disabled",
                                 background="#1e1e1e", foreground="#d4d4d4",
                                 insertbackground="#d4d4d4")
        self.text.grid(row=1, column=0, sticky="nsew")
        self.text.tag_configure("info", foreground="#d4d4d4")
        self.text.tag_configure("ok", foreground="#7ec699")
        self.text.tag_configure("warn", foreground="#e6b455")
        self.text.tag_configure("error", foreground="#ff8a80")
        self.text.tag_configure("dim", foreground="#8a8a8a")

    def append(self, line, tag=None):
        line = core.clip_for_display(line)
        tag = tag or classify_line(line)
        self.text.configure(state="normal")
        self.text.insert("end", line + "\n", tag)
        lines = int(self.text.index("end-1c").split(".")[0])
        if lines > LOG_LINE_LIMIT:
            self.text.delete("1.0", "%d.0" % (lines - LOG_LINE_LIMIT))
        self.text.configure(state="disabled")
        if self.autoscroll.get():
            self.text.see("end")

    def clear(self):
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.configure(state="disabled")


class SettingsDialog(tk.Toplevel):
    """串口 / 账户 / QMT / VK 设置；保存到 .env，密码只写入文件不回显。"""

    def __init__(self, app):
        super().__init__(app.root)
        self.app = app
        self.config = app.config
        self.title("设置")
        self.transient(app.root)
        self.resizable(False, False)
        self._build()
        self.wait_visibility()
        self.grab_set()
        self.bind("<Escape>", lambda _event: self.destroy())
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    # ------------------------------------------------------------ layout
    def _label_entry(self, parent, row, label, variable, width=42, show=None,
                     browse=None):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w",
                                           padx=(0, 8), pady=3)
        entry = ttk.Entry(parent, textvariable=variable, width=width, show=show)
        entry.grid(row=row, column=1, sticky="ew", pady=3)
        if browse:
            ttk.Button(parent, text="浏览…", width=7, command=browse).grid(
                row=row, column=2, sticky="w", padx=(6, 0), pady=3)
        return entry

    def _build(self):
        cfg = self.config
        frame = ttk.Frame(self, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(0, weight=1)

        self.port = tk.StringVar(value=cfg.port)
        self.baudrate = tk.StringVar(value=str(cfg.baudrate))
        self.max_frame = tk.StringVar(value=str(cfg.max_frame_bytes))
        self.pubsub = tk.BooleanVar(value=cfg.pubsub)
        self.account = tk.StringVar(value=cfg.account)
        self.login_user = tk.StringVar(value=cfg.login_user)
        self.login_password = tk.StringVar(value=cfg.login_password)
        self.qmt_dir = tk.StringVar(value=cfg.qmt_root)
        self.qmt_mode = tk.StringVar(value=cfg.qmt_mode)
        self.qmt_exe = tk.StringVar(value=cfg.qmt_exe)
        self.vk_host = tk.StringVar(value=cfg.vk_host)
        self.vk_port = tk.StringVar(value=str(cfg.vk_port))
        self.vk_db = tk.StringVar(value=str(cfg.vk_db))
        self.vk_user = tk.StringVar(value=cfg.vk_username)
        self.vk_password = tk.StringVar(value=cfg.vk_password)
        self.vk_timeout = tk.StringVar(value=str(cfg.vk_timeout))

        row = 0
        serial = ttk.LabelFrame(frame, text="串口", padding=8)
        serial.grid(row=row, column=0, sticky="ew", pady=(0, 8))
        serial.columnconfigure(1, weight=1)
        self._label_entry(serial, 0, "串口", self.port)
        self._label_entry(serial, 1, "波特率", self.baudrate)
        self._label_entry(serial, 2, "单帧上限（字节）", self.max_frame)
        ttk.Checkbutton(serial, text="客户端用 VK pub/sub 发请求（--vk-pubsub）",
                        variable=self.pubsub).grid(row=3, column=0, columnspan=2,
                                                 sticky="w", pady=(4, 0))

        row += 1
        account = ttk.LabelFrame(frame, text="BIGQMT 账户", padding=8)
        account.grid(row=row, column=0, sticky="ew", pady=(0, 8))
        account.columnconfigure(1, weight=1)
        self._label_entry(account, 0, "资金账号", self.account)
        self._label_entry(account, 1, "QMT 登录账号", self.login_user)
        self._label_entry(account, 2, "QMT 登录密码", self.login_password, show="*")
        ttk.Label(account, text="登录账号留空时使用资金账号；密码只写入 .env，不会显示在日志里。",
                  foreground="#777777").grid(row=3, column=0, columnspan=3,
                                             sticky="w", pady=(4, 0))

        row += 1
        qmt = ttk.LabelFrame(frame, text="QMT 软件目录", padding=8)
        qmt.grid(row=row, column=0, sticky="ew", pady=(0, 8))
        qmt.columnconfigure(1, weight=1)
        self._label_entry(qmt, 0, "目录", self.qmt_dir, browse=self._browse_qmt_dir)
        ttk.Label(qmt, text="启动方式").grid(row=1, column=0, sticky="w",
                                           padx=(0, 8), pady=3)
        ttk.Combobox(qmt, textvariable=self.qmt_mode, values=("exe", "login"),
                     state="readonly", width=16).grid(row=1, column=1, sticky="w", pady=3)
        self._label_entry(qmt, 2, "可执行文件", self.qmt_exe)
        ttk.Label(qmt, text="exe：依赖 QMT 自己的自动登录；login：由 qmt_launcher 代输账号密码。",
                  foreground="#777777").grid(row=3, column=0, columnspan=3,
                                             sticky="w", pady=(4, 0))

        row += 1
        vk_frame = ttk.LabelFrame(frame, text="VK", padding=8)
        vk_frame.grid(row=row, column=0, sticky="ew", pady=(0, 8))
        vk_frame.columnconfigure(1, weight=1)
        self._label_entry(vk_frame, 0, "地址 / 主机", self.vk_host)
        self._label_entry(vk_frame, 1, "端口", self.vk_port)
        self._label_entry(vk_frame, 2, "DB", self.vk_db)
        self._label_entry(vk_frame, 3, "用户名（可空）", self.vk_user)
        self._label_entry(vk_frame, 4, "密码（可空）", self.vk_password, show="*")
        self._label_entry(vk_frame, 5, "桥接命令超时（秒）", self.vk_timeout)
        ttk.Label(vk_frame, text="地址可填主机名或完整 valkey:// URL；用户名 / 密码留空表示无认证。",
                  foreground="#777777").grid(row=6, column=0, columnspan=3,
                                             sticky="w", pady=(4, 0))

        self.preview_var = tk.StringVar()
        ttk.Label(frame, textvariable=self.preview_var, foreground="#777777",
                  wraplength=520, justify="left").grid(row=row + 1, column=0,
                                                        sticky="w", pady=(0, 6))
        for variable in (self.vk_host, self.vk_port, self.vk_db,
                         self.vk_user, self.vk_password):
            variable.trace_add("write", lambda *_args: self._update_preview())
        self._update_preview()

        self.target_var = tk.StringVar(value="保存到：" + self.app.env_target())
        ttk.Label(frame, textvariable=self.target_var, foreground="#777777",
                  wraplength=520, justify="left").grid(row=row + 2, column=0,
                                                        sticky="w", pady=(0, 8))

        buttons = ttk.Frame(frame)
        buttons.grid(row=row + 3, column=0, sticky="e")
        ttk.Button(buttons, text="取消", width=10,
                   command=self.destroy).pack(side="right", padx=(8, 0))
        ttk.Button(buttons, text="保存", width=10,
                   command=self._on_save).pack(side="right")

    def _browse_qmt_dir(self):
        initial = self.qmt_dir.get().strip() or self.config.qmt_root or os.path.expanduser("~")
        chosen = filedialog.askdirectory(parent=self, initialdir=initial,
                                         title="选择 QMT 安装目录")
        if chosen:
            self.qmt_dir.set(core.normalize_qmt_root(chosen))

    def _vk_fields(self):
        raw_host = self.vk_host.get().strip()
        if "://" in raw_host:
            parts = core.parse_vk_url(raw_host)
            return raw_host, parts["host"], parts["port"], parts["db"], \
                parts["username"], parts["password"]
        host = raw_host or "127.0.0.1"
        port = int(self.vk_port.get().strip() or 6379)
        db = int(self.vk_db.get().strip() or 0)
        if not 0 < port < 65536:
            raise ValueError("VK 端口必须在 1-65535 之间")
        if db < 0:
            raise ValueError("VK DB 不能为负数")
        username = self.vk_user.get()
        password = self.vk_password.get()
        return core.build_vk_url(host, port, db, username, password), \
            host, port, db, username, password

    def _update_preview(self):
        try:
            url, _host, _port, _db, _user, _password = self._vk_fields()
        except (ValueError, TypeError):
            self.preview_var.set("连接串：端口 / DB 需要是数字")
            return
        self.preview_var.set("连接串：" + core.redact_url(url))

    def _on_save(self):
        try:
            port = self.port.get().strip()
            if not port:
                raise ValueError("串口不能为空")
            baudrate = int(self.baudrate.get().strip())
            max_frame = int(self.max_frame.get().strip())
            if baudrate <= 0 or max_frame <= 0:
                raise ValueError("波特率和帧上限必须大于 0")
            account = self.account.get().strip()
            if not account:
                raise ValueError("BIGQMT 资金账号不能为空")
            qmt_dir = core.normalize_qmt_root(self.qmt_dir.get().strip())
            if not qmt_dir:
                raise ValueError("QMT 软件目录不能为空")
            qmt_mode = self.qmt_mode.get()
            qmt_exe = self.qmt_exe.get().strip() or core.DEFAULT_QMT_EXE
            login_user = self.login_user.get().strip() or account
            login_password = self.login_password.get()
            if qmt_mode == "login" and not login_password:
                raise ValueError("login 模式需要 QMT 登录密码")
            vk_timeout = core.parse_vk_timeout(self.vk_timeout.get())
            url, host, vk_port, db, username, password = self._vk_fields()
        except (ValueError, TypeError) as exc:
            messagebox.showerror("设置无效", str(exc), parent=self)
            return

        target = self.app.env_target()
        values = {
            "BIGQMT_ACCOUNT_ID": account,
            "BIGQMT_LOGIN_USER": login_user,
            "BIGQMT_LOGIN_PASSWORD": login_password,
            "BIGQMT_QMT_DIR": qmt_dir,
            "BIGQMT_QMT_MODE": qmt_mode,
            "BIGQMT_QMT_EXE": qmt_exe,
            "BIGQMT_SERIAL_PORT": port,
            "BIGQMT_SERIAL_BAUDRATE": str(baudrate),
            "BIGQMT_SERIAL_MAX_FRAME_BYTES": str(max_frame),
            "BIGQMT_SERIAL_VK_PUBSUB": "1" if self.pubsub.get() else "0",
            "BIGQMT_SERIAL_VK_TIMEOUT": str(vk_timeout),
            "BIGQMT_VK_HOST": host,
            "BIGQMT_VK_PORT": str(vk_port),
            "BIGQMT_VK_DB": str(db),
            "BIGQMT_VK_USERNAME": username,
            "BIGQMT_VK_PASSWORD": password,
            "BIGQMT_SERIAL_VK_URL": url,
            "BIGQMT_VK_URL": url,
        }
        try:
            core.update_env_file(target, values)
        except OSError as exc:
            messagebox.showerror("保存失败", str(exc), parent=self)
            return

        exe_path = os.path.join(qmt_dir, "bin.x64", qmt_exe)
        if not os.path.isfile(exe_path):
            messagebox.showwarning(
                "QMT 目录", "未找到 %s；请确认目录和可执行文件名。" % exe_path,
                parent=self)

        settings = {
            "env_file": target,
            "account": account,
            "login_user": login_user,
            "login_password": login_password,
            "qmt_dir": qmt_dir,
            "qmt_mode": qmt_mode,
            "qmt_exe": qmt_exe,
            "port": port,
            "baudrate": baudrate,
            "max_frame_bytes": max_frame,
            "pubsub": bool(self.pubsub.get()),
            "vk_url": url,
            "vk_host": host,
            "vk_port": vk_port,
            "vk_db": db,
            "vk_username": username,
            "vk_password": password,
            "vk_timeout": vk_timeout,
        }
        self.destroy()
        self.app.apply_settings(settings)


class SerialControllerApp(object):
    def __init__(self, root, config, autostart=False, pending_stop_pids=None,
                 restart_qmt_on_start=False, stop_qmt_on_stop=True,
                 auto_restart_qmt=True):
        self._startup_restart_qmt = bool(restart_qmt_on_start)
        self._startup_stop_qmt = bool(stop_qmt_on_stop)
        self._startup_auto_restart = bool(auto_restart_qmt)
        self._pending_stop_pids = list(pending_stop_pids or [])
        self.root = root
        self.config = config
        self.events = queue.Queue()
        self.lock = threading.Lock()
        self.closing = False
        self._closing_event = threading.Event()

        self.bridge_proc = None
        self.bridge_state = "stopped"
        self.bridge_ready = threading.Event()
        self.bridge_exited = threading.Event()
        self._bridge_watchdog_active = False
        self.session_ready = False
        self.qmt_running = False
        self.qmt_pids = []
        self.qmt_aux_count = 0
        self.external_bridges = []
        self.busy = False
        self.python_command = []
        self.python_desc = ""
        self.last_bridge_error = ""
        self._start_options = {"restart_qmt": False, "stop_qmt": True,
                               "existing_bridges": []}

        # QMT watchdog：启动成功后监控主进程，退出后自动拉起。
        self._auto_restart_enabled = self._startup_auto_restart
        self._watchdog_active = False
        self._watchdog_suspended = False
        self._qmt_restarting = False
        self._qmt_missing_since = None
        self._qmt_alive_since = None
        self._qmt_next_attempt = 0.0
        self._qmt_restart_attempts = 0
        self._qmt_restart_window_start = None
        self._qmt_missing_grace = 8.0

        self._build_ui()
        self._refresh_state()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(120, self._drain_events)
        self.root.after(BRIDGE_CHECK_INTERVAL_MS, self._check_bridge_process)

        threading.Thread(target=self._monitor_qmt_loop, daemon=True).start()
        threading.Thread(target=self._qmt_log_loop, daemon=True).start()
        if autostart:
            self.root.after(600, self._on_start)

    # ------------------------------------------------------------ UI layout
    def _build_ui(self):
        self.root.title("Big QMT 串口桥接控制台")
        self.root.geometry("1120x760")
        self.root.minsize(880, 580)
        with suppress(tk.TclError):
            ttk.Style().theme_use("vista")

        main = ttk.Frame(self.root, padding=10)
        main.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main.columnconfigure(0, weight=1)
        main.rowconfigure(3, weight=1)

        top = ttk.Frame(main)
        top.grid(row=0, column=0, sticky="ew")
        self.start_button = ttk.Button(top, text="启动", width=10,
                                       command=self._on_start)
        self.start_button.pack(side="left", padx=(0, 6))
        self.stop_button = ttk.Button(top, text="停止", width=10,
                                      command=self._on_stop)
        self.stop_button.pack(side="left", padx=(0, 6))
        self.restart_button = ttk.Button(top, text="重启", width=10,
                                         command=self._on_restart)
        self.restart_button.pack(side="left", padx=(0, 10))
        self.settings_button = ttk.Button(top, text="设置…", width=10,
                                          command=self._on_settings)
        self.settings_button.pack(side="left", padx=(0, 18))

        self.restart_qmt_on_start = tk.BooleanVar(value=self._startup_restart_qmt)
        self.stop_qmt_on_stop = tk.BooleanVar(value=self._startup_stop_qmt)
        self.auto_restart_qmt = tk.BooleanVar(value=self._startup_auto_restart)
        ttk.Checkbutton(top, text="启动时重启已运行的 QMT",
                        variable=self.restart_qmt_on_start).pack(side="left", padx=(0, 12))
        ttk.Checkbutton(top, text="停止/退出时关闭 QMT",
                        variable=self.stop_qmt_on_stop).pack(side="left", padx=(0, 12))
        ttk.Checkbutton(top, text="QMT 退出后自动拉起",
                        variable=self.auto_restart_qmt,
                        command=self._on_auto_restart_toggle).pack(side="left")

        status = ttk.Frame(main)
        status.grid(row=1, column=0, sticky="ew", pady=(10, 2))
        self.bridge_var = tk.StringVar(value="桥接：已停止")
        self.qmt_var = tk.StringVar(value="QMT：未知")
        self.session_var = tk.StringVar(value="串口会话：未就绪")
        self.bridge_label = ttk.Label(status, textvariable=self.bridge_var)
        self.bridge_label.pack(side="left", padx=(0, 22))
        self.qmt_label = ttk.Label(status, textvariable=self.qmt_var)
        self.qmt_label.pack(side="left", padx=(0, 22))
        self.session_label = ttk.Label(status, textvariable=self.session_var)
        self.session_label.pack(side="left")
        self.elevation_var = tk.StringVar(
            value="权限：管理员" if core.is_admin() else "权限：普通用户")
        self.elevation_label = ttk.Label(status, textvariable=self.elevation_var)
        self.elevation_label.pack(side="left", padx=(22, 0))
        self.elevation_label.configure(
            foreground="#1b5e20" if core.is_admin() else "#8a5a00")

        self.hint_var = tk.StringVar(value="就绪。点击「启动」：先启动桥接，串口打开后再拉起 QMT。")
        hint = ttk.Label(main, textvariable=self.hint_var, foreground="#555555",
                         wraplength=1060, justify="left")
        hint.grid(row=2, column=0, sticky="ew", pady=(4, 0))

        self.notebook = ttk.Notebook(main)
        self.bridge_pane = LogPane(self.notebook)
        self.qmt_pane = LogPane(self.notebook)
        self.notebook.add(self.bridge_pane, text="serial_vk_bridge 日志")
        self.notebook.add(self.qmt_pane, text="QMT 策略日志（FormulaOutput）")
        self.notebook_visible = False

        footer = ttk.Frame(main)
        footer.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        self.config_var = tk.StringVar(value=self._config_line())
        ttk.Label(footer, textvariable=self.config_var, foreground="#777777",
                  wraplength=1060, justify="left").pack(side="left")

        self._append_config_notes()

    def _config_line(self):
        env = self.config.env_file or "(未找到 .env)"
        return "配置：%s    .env：%s" % (self.config.summary(), env)

    def _append_config_notes(self):
        problems = []
        if not self.config.account:
            problems.append("缺少资金账号（.env 的 BIGQMT_ACCOUNT_ID）")
        if not self.config.qmt_root:
            problems.append("缺少 QMT 目录（.env 的 BIGQMT_QMT_DIR 或 --qmt-dir）")
        if problems:
            self.hint_var.set("；".join(problems) + "。点「设置…」补齐后保存。")
        elif not core.is_admin():
            exe = self._qmt_exe_path()
            if exe and core.qmt_requires_elevation(exe):
                self.hint_var.set(
                    "QMT（%s）的 manifest 要求管理员权限；点「启动」会提示以管理员身份"
                    "重启本控制台（需要确认 UAC）。" % os.path.basename(exe))
        if self.config.local_config:
            self.config_var.set(self._config_line() + "    VK 来自 " + self.config.local_config)

    def env_target(self):
        return self.config.env_file or core.default_env_path()

    def _on_auto_restart_toggle(self):
        self._auto_restart_enabled = bool(self.auto_restart_qmt.get())
        with self.lock:
            self._watchdog_suspended = False
            self._qmt_restart_attempts = 0
            self._qmt_restart_window_start = None
            self._qmt_missing_since = None
            self._qmt_next_attempt = 0.0
        if self._auto_restart_enabled:
            self._post("hint", "QMT 退出后自动拉起已开启。")
        else:
            self._post("hint", "QMT 退出后自动拉起已关闭。")

    def _watchdog_eligible(self, main_running):
        if main_running:
            return False
        if not self._watchdog_active or not self._auto_restart_enabled:
            return False
        if self._watchdog_suspended or self._qmt_restarting or self.busy:
            return False
        if self.closing:
            return False
        proc = self.bridge_proc
        return proc is not None and proc.poll() is None

    def _maybe_watchdog(self, main_running):
        """由监控线程调用：QMT 主进程持续缺失时拉起一次。"""
        now = time.monotonic()
        with self.lock:
            if main_running:
                self._qmt_missing_since = None
                if self._qmt_alive_since is None:
                    self._qmt_alive_since = now
                if (self._qmt_restart_attempts
                        and now - self._qmt_alive_since >= 60.0):
                    self._qmt_restart_attempts = 0
                    self._qmt_restart_window_start = None
                return
            self._qmt_alive_since = None
            if not self._watchdog_eligible(False):
                self._qmt_missing_since = None
                return
            if self._qmt_missing_since is None:
                self._qmt_missing_since = now
                return
            if now - self._qmt_missing_since < self._qmt_missing_grace:
                return
            if now < self._qmt_next_attempt:
                return
            self._qmt_restarting = True
            self._qmt_missing_since = None
        threading.Thread(target=self._qmt_watchdog_worker, daemon=True).start()

    def _qmt_watchdog_worker(self):
        attempts = 0
        try:
            now = time.monotonic()
            with self.lock:
                if (self._qmt_restart_window_start is None
                        or now - self._qmt_restart_window_start > 600.0):
                    self._qmt_restart_window_start = now
                    self._qmt_restart_attempts = 0
                self._qmt_restart_attempts += 1
                attempts = self._qmt_restart_attempts
                self.session_ready = False
            self._post("bridge", "[监控] 检测到 QMT 主进程已退出，正在自动拉起（第 %d 次）…"
                       % attempts, "warn")
            self._post("hint", "QMT 已退出，正在自动拉起（第 %d 次）…" % attempts)
            self._post("state")

            # 只清理残留的子进程；如果主进程又出现，说明用户已手动启动，取消本次拉起。
            try:
                procs = core.running_qmt_processes(self.config.qmt_root)
            except Exception:
                procs = []
            main_name = self.config.qmt_exe.lower()
            if any(item[1].lower() == main_name for item in procs):
                self._post("bridge", "[监控] QMT 主进程已重新出现，取消本次拉起。", "dim")
                return
            aux = [item for item in procs if item[1].lower() != main_name]
            if aux:
                self._post("bridge", "[监控] 清理残留的 QMT 子进程：%s"
                           % [item[0] for item in aux], "warn")
                self._qmt_action("close", timeout=120)
                time.sleep(0.5)

            proc = self.bridge_proc
            if (not self._watchdog_active or self.closing
                    or proc is None or proc.poll() is not None):
                self._post("bridge", "[监控] 桥接已停止或程序正在退出，取消自动拉起。", "dim")
                return

            code, output = self._qmt_action(
                "open", extra=["--mode", self.config.qmt_mode,
                               "--exe", self.config.qmt_exe, "--no-wait"],
                timeout=240)
            if code == 0:
                self._post("bridge", "[监控] QMT 已重新拉起，等待策略重连串口…", "ok")
                self._post("hint", "QMT 已重新拉起，等待策略连上串口…")
                return

            self._post("bridge", "[监控] 自动拉起 QMT 失败，exit=%s" % code, "error")
            if "WinError 740" in output or "管理员" in output:
                with self.lock:
                    self._watchdog_suspended = True
                self._post("hint", "自动拉起失败：QMT 需要管理员权限，请以管理员身份运行本程序。")
            elif attempts >= 6:
                with self.lock:
                    self._watchdog_suspended = True
                    self._auto_restart_enabled = False
                self._post("watchdog_off")
                self._post("message", (
                    "warning", "自动拉起已暂停",
                    "QMT 连续 %d 次拉起失败，已暂停自动拉起。请检查 QMT 登录/策略后"
                    "勾选「QMT 退出后自动拉起」恢复，或手动重启。" % attempts))
        finally:
            with self.lock:
                self._qmt_restarting = False
                self._qmt_next_attempt = time.monotonic() + min(
                    30.0, 5.0 * max(1, self._qmt_restart_attempts))

    def _on_settings(self):
        if self.busy:
            return
        SettingsDialog(self)

    def apply_settings(self, settings):
        old_root = self.config.qmt_root
        config = self.config
        for key in ("account", "login_user", "login_password",
                    "qmt_mode", "qmt_exe", "port", "baudrate",
                    "max_frame_bytes", "pubsub", "vk_url", "vk_host",
                    "vk_port", "vk_db", "vk_username",
                    "vk_password", "vk_timeout"):
            setattr(config, key, settings[key])
        config.qmt_root = core.normalize_qmt_root(settings["qmt_dir"])
        config.env_file = settings["env_file"]
        config.env_dir = os.path.dirname(config.env_file) or config.env_dir
        config.local_config = ""
        config.dotenv.update({
            "BIGQMT_ACCOUNT_ID": config.account,
            "BIGQMT_LOGIN_USER": config.login_user,
            "BIGQMT_LOGIN_PASSWORD": config.login_password,
            "BIGQMT_QMT_DIR": config.qmt_root,
            "BIGQMT_QMT_MODE": config.qmt_mode,
            "BIGQMT_QMT_EXE": config.qmt_exe,
            "BIGQMT_SERIAL_PORT": config.port,
            "BIGQMT_SERIAL_BAUDRATE": str(config.baudrate),
            "BIGQMT_SERIAL_MAX_FRAME_BYTES": str(config.max_frame_bytes),
            "BIGQMT_SERIAL_VK_PUBSUB": "1" if config.pubsub else "0",
            "BIGQMT_SERIAL_VK_TIMEOUT": str(config.vk_timeout),
            "BIGQMT_VK_HOST": config.vk_host,
            "BIGQMT_VK_PORT": str(config.vk_port),
            "BIGQMT_VK_DB": str(config.vk_db),
            "BIGQMT_VK_USERNAME": config.vk_username,
            "BIGQMT_VK_PASSWORD": config.vk_password,
            "BIGQMT_SERIAL_VK_URL": config.vk_url,
            "BIGQMT_VK_URL": config.vk_url,
        })
        self.config_var.set(self._config_line())
        self._refresh_state()

        running = self.bridge_proc is not None and self.bridge_proc.poll() is None
        if not running:
            self.hint_var.set("设置已保存：%s。" % config.env_file)
            return
        if old_root != config.qmt_root:
            self.hint_var.set(
                "设置已保存；QMT 目录已改变。请先停止旧 QMT，再点「重启」。")
            messagebox.showinfo(
                "设置已保存",
                "QMT 软件目录已改变。旧 QMT 仍需手动停止，然后再点「重启」。")
            return
        if messagebox.askyesno("设置已保存",
                               "桥接正在运行。现在重启使设置生效吗？"):
            self._on_restart()
        else:
            self.hint_var.set("设置已保存，重启桥接后生效。")

    # --------------------------------------------------------------- events
    def _post(self, kind, payload=None, tag=None):
        if self.closing:
            return
        self.events.put((kind, payload, tag))

    def _drain_events(self):
        for _unused in range(3000):
            try:
                kind, payload, tag = self.events.get_nowait()
            except queue.Empty:
                break
            if kind == "bridge":
                self.bridge_pane.append(payload, tag)
            elif kind == "qmt":
                self.qmt_pane.append(payload, tag)
            elif kind == "state":
                self._refresh_state()
            elif kind == "hint":
                self.hint_var.set(payload)
            elif kind == "message":
                level, title, text = payload
                shower = {"error": messagebox.showerror,
                          "warning": messagebox.showwarning,
                          "info": messagebox.showinfo}.get(level, messagebox.showinfo)
                shower(title, text)
            elif kind == "request_start":
                self._on_start()
            elif kind == "watchdog_off":
                self.auto_restart_qmt.set(False)
                self._auto_restart_enabled = False
            elif kind == "bridge_exit":
                self._on_bridge_exit(*payload)
        if not self.closing:
            self.root.after(100, self._drain_events)

    def _show_tabs(self):
        if self.notebook_visible:
            return
        self.notebook.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        self.notebook_visible = True

    def _set_busy(self, busy):
        self.busy = busy
        self._post("state")

    def _refresh_state(self):
        running = self.bridge_proc is not None and self.bridge_proc.poll() is None
        if self.bridge_state == "starting":
            self.bridge_var.set("桥接：启动中…")
        elif self.bridge_state == "stopping":
            self.bridge_var.set("桥接：停止中…")
        elif running:
            self.bridge_var.set("桥接：运行中 (PID %s)" % self.bridge_proc.pid)
        else:
            self.bridge_var.set("桥接：已停止")

        if self.qmt_running:
            pids = ",".join(str(pid) for pid in self.qmt_pids) or "?"
            self.qmt_var.set("QMT：运行中 (PID %s)" % pids)
        elif self.qmt_aux_count:
            self.qmt_var.set("QMT：主进程已退出（残留子进程）")
        else:
            self.qmt_var.set("QMT：已停止")

        self.session_var.set("串口会话：已就绪" if self.session_ready else "串口会话：未就绪")
        self.session_label.configure(
            foreground="#1b5e20" if self.session_ready else "#555555")
        self.qmt_label.configure(foreground="#1b5e20" if self.qmt_running else "#555555")
        self.bridge_label.configure(foreground="#1b5e20" if running else "#555555")

        if self.busy:
            state = "disabled"
        elif running:
            state = "disabled"
        else:
            state = "normal"
        self.start_button.configure(
            state="disabled" if (state == "disabled" or not self._can_start()) else "normal")
        stop_needed = (self._bridge_watchdog_active or running
                       or self.qmt_running or bool(self.external_bridges)
                       or self.qmt_aux_count > 0)
        self.stop_button.configure(
            state="disabled" if (self.busy or not stop_needed) else "normal")
        self.restart_button.configure(state=state)
        self.settings_button.configure(state="disabled" if self.busy else "normal")

    def _can_start(self):
        return bool(self.config.account and self.config.qmt_root)

    # ------------------------------------------------------------- buttons
    def _qmt_exe_path(self):
        if not self.config.qmt_root:
            return ""
        return os.path.join(self.config.qmt_root, "bin.x64", self.config.qmt_exe)

    def _needs_elevation_for_start(self):
        if core.is_admin():
            return False
        exe = self._qmt_exe_path()
        if not exe or not core.qmt_requires_elevation(exe):
            return False
        try:
            running = bool(core.running_qmt_processes(self.config.qmt_root))
        except Exception:
            running = False
        if running and not self.restart_qmt_on_start.get():
            return False  # 本次只启动桥接，不动 QMT
        return True

    def _relaunch_elevated(self, existing_bridges=None):
        extra = ["--autostart"]
        if self.restart_qmt_on_start.get():
            extra.append("--restart-qmt")
        if not self.stop_qmt_on_stop.get():
            extra.append("--keep-qmt")
        if not self.auto_restart_qmt.get():
            extra.append("--no-auto-restart")
        if existing_bridges:
            extra += ["--stop-bridge-pids",
                      ",".join(str(item["pid"]) for item in existing_bridges)]
        if not core.relaunch_as_admin(extra):
            return False
        proc = self._begin_close()
        if proc is not None and proc.poll() is None:
            with suppress(OSError):
                proc.terminate()
        self.root.destroy()
        return True

    def _on_start(self):
        if self.busy:
            return
        if not self._can_start():
            messagebox.showerror(
                "配置不完整",
                "启动需要资金账号和 QMT 目录。\n请在 .env 设置 BIGQMT_ACCOUNT_ID、"
                "BIGQMT_QMT_DIR，或用 --qmt-dir 指定。")
            return
        with self.lock:
            if self.bridge_proc is not None and self.bridge_proc.poll() is None:
                return
        pending_pids = self._pending_stop_pids
        self._pending_stop_pids = []
        try:
            detected = core.find_bridge_processes(self.config.port)
        except Exception:
            detected = []
        detected = [item for item in detected
                    if self.bridge_proc is None or item["pid"] != self.bridge_proc.pid]
        if pending_pids:
            existing = [item for item in detected if item["pid"] in pending_pids]
        else:
            existing = detected
            if existing:
                details = "\n".join("PID %s  %s" % (item["pid"], item["cmdline"][:180])
                                     for item in existing[:5])
                if not messagebox.askyesno(
                        "串口可能被占用",
                        "检测到已经在运行的桥接进程正在使用 %s：\n\n%s\n\n先停止它再启动吗？"
                        % (self.config.port, details)):
                    return
            else:
                existing = []
        if self._needs_elevation_for_start():
            if not messagebox.askyesno(
                    "需要管理员权限",
                    "QMT（%s）的 manifest 要求管理员权限（WinError 740）。\n\n"
                    "是否以管理员身份重新启动本控制台？\n"
                    "确认 UAC 后，新窗口会自动开始启动。"
                    % os.path.basename(self._qmt_exe_path())):
                self.hint_var.set(
                    "未提升权限：QMT 需要以管理员身份运行，本次未启动。")
                return
            if self._relaunch_elevated(existing):
                return
            messagebox.showerror(
                "无法提升权限",
                "UAC 被取消或提升失败。请关闭本程序，右键选择「以管理员身份运行」。")
            return
        self._start_options = {
            "restart_qmt": bool(self.restart_qmt_on_start.get()),
            "stop_qmt": bool(self.stop_qmt_on_stop.get()),
            "existing_bridges": existing,
        }
        self._auto_restart_enabled = bool(self.auto_restart_qmt.get())
        with self.lock:
            self._bridge_watchdog_active = False
            self._watchdog_active = False
            self._watchdog_suspended = False
            self._qmt_restart_attempts = 0
            self._qmt_restart_window_start = None
            self._qmt_missing_since = None
            self._qmt_alive_since = None
            self._qmt_next_attempt = 0.0
        self._show_tabs()
        self.notebook.select(self.bridge_pane)
        self.bridge_state = "starting"
        self._set_busy(True)
        self.hint_var.set("正在启动桥接…")
        threading.Thread(target=self._start_worker, daemon=True).start()

    def _on_stop(self):
        if self.busy:
            return
        close_qmt = bool(self.stop_qmt_on_stop.get())
        self._bridge_watchdog_active = False
        self.bridge_state = "stopping"
        self._set_busy(True)
        self.hint_var.set("正在停止…")
        threading.Thread(target=self._stop_worker, args=(close_qmt,),
                         daemon=True).start()

    def _on_restart(self):
        if self.busy:
            return
        close_qmt = bool(self.stop_qmt_on_stop.get())
        self._bridge_watchdog_active = False
        self.bridge_state = "stopping"
        self._set_busy(True)
        self.hint_var.set("正在重启…")
        threading.Thread(target=self._restart_worker, args=(close_qmt,),
                         daemon=True).start()

    # ------------------------------------------------------------- workers
    def _start_worker(self, bridge_only=False):
        try:
            if bridge_only and core.find_bridge_processes(self.config.port):
                raise core.ConfigError("检测到其他桥接进程正在使用 %s" % self.config.port)
            existing = [] if bridge_only else self._start_options["existing_bridges"]
            if existing:
                pids = [item["pid"] for item in existing]
                self._post("bridge", "[控制台] 停止占用 %s 的旧桥接进程：%s"
                           % (self.config.port, pids), "warn")
                core.terminate_bridge_processes(existing)
                time.sleep(0.6)
            python_command, desc = core.resolve_python(self.config.python or None)
            self.python_command = python_command
            self.python_desc = desc
            self._post("bridge", "[控制台] 使用 Python：%s" % desc, "dim")

            command = core.bridge_command(python_command, self.config)
            self._post("bridge", "[控制台] 启动桥接：%s" % self._display_command(command), "dim")
            env = core.child_env(self.config)
            with self.lock:
                if self.closing:
                    return
                proc = subprocess.Popen(
                    command, cwd=self.config.env_dir, env=env,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    creationflags=core.CREATE_NO_WINDOW | core.CREATE_NEW_PROCESS_GROUP)
                self.bridge_proc = proc
                self.bridge_ready.clear()
                self.bridge_exited.clear()
                self.last_bridge_error = ""
                self.session_ready = False
            self._post("state")
            threading.Thread(target=self._pump_bridge, args=(proc,), daemon=True).start()

            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if self.bridge_ready.wait(0.2):
                    break
                if self.bridge_exited.is_set():
                    break
            if not self.bridge_ready.is_set() or proc.poll() is not None:
                self._terminate_bridge(proc)
                code = proc.poll()
                detail = self.last_bridge_error
                message = "桥接没有打开串口（exit=%s）。" % code
                if detail:
                    message += "\n\n" + detail
                message += ("\n\n如果提示「拒绝访问」，通常是旧的桥接进程、串口工具或 QMT 占用了 %s。"
                            "桥接应使用 QMT 串口（如 COM10）的另一端（如 COM11），"
                            "端口可在「设置…」里修改。旧桥接如果是以管理员权限运行的，"
                            "本程序也需要以管理员权限运行才能停止它。" % self.config.port)
                raise core.ConfigError(message)
            with self.lock:
                if self.closing:
                    return
                self._bridge_watchdog_active = True
                self.bridge_state = "running"
            self._post("state")
            if bridge_only:
                self._post("bridge", "[监控] 桥接已自动重启（PID %s）。" % proc.pid, "ok")
            else:
                self._handle_qmt_after_bridge(proc)
                with self.lock:
                    if not self.closing:
                        self._watchdog_active = True
                        self._qmt_missing_since = None
                        self._qmt_alive_since = None
            self._post("hint", "桥接已运行。等待 QMT 策略连接串口…")
        except Exception as exc:
            if bridge_only:
                self._set_bridge_stopped()
                self._post("bridge", "[监控] 自动启动桥接失败：%s" % exc, "error")
                self._post("hint", "桥接自动重启失败，下一次检查时重试。")
                return
            self._post("bridge", "[控制台] %s" % exc, "error")
            self._post("message", ("error", "启动失败", str(exc)))
            if self.bridge_proc is not None and self.bridge_proc.poll() is None:
                self.bridge_state = "running"
                self._post("hint", "桥接仍在运行，但 QMT 启动失败：%s" % exc)
            else:
                self.bridge_state = "stopped"
                self._post("hint", "启动失败：%s" % exc)
        finally:
            self._set_busy(False)

    def _handle_qmt_after_bridge(self, proc):
        if not self.config.qmt_root:
            return
        procs = core.running_qmt_processes(self.config.qmt_root)
        if procs:
            if self._start_options.get("restart_qmt"):
                self._post("bridge", "[控制台] QMT 正在运行，按选项先关闭再启动…", "warn")
                code, _output = self._qmt_action("close", timeout=180)
                if code != 0:
                    raise core.ConfigError("关闭已有 QMT 失败，qmt_launcher exit=%s" % code)
                self._wait_qmt_stopped(60)
            else:
                self._post(
                    "bridge",
                    "[控制台] QMT 已在运行。若串口会话未恢复，可重载 QMT 策略，"
                    "或勾选「启动时重启已运行的 QMT」后点「重启」。",
                    "warn")
                return

        mode = self.config.qmt_mode
        self._post("bridge", "[控制台] 启动 QMT（%s）…" % mode)
        code, output = self._qmt_action(
            "open", extra=["--mode", mode, "--exe", self.config.qmt_exe, "--no-wait"],
            timeout=240)
        if code != 0:
            if ("WinError 740" in output or "需要提升" in output
                    or "管理员权限" in output):
                raise core.ConfigError(
                    "QMT 要求管理员权限（WinError 740）。\n\n"
                    "请关闭本程序，右键选择「以管理员身份运行」后再启动。")
            raise core.ConfigError("QMT 启动失败，qmt_launcher exit=%s" % code)
        self._post("bridge", "[控制台] QMT 已启动，等待自动登录并运行策略。", "ok")
        self._post("hint", "QMT 启动中：看到「QMT 串口会话已就绪」即表示策略已连上桥接。")

    def _pump_bridge(self, proc):
        try:
            for raw in iter(proc.stdout.readline, b""):
                line = core.decode_text(raw).rstrip("\r\n")
                if not line:
                    continue
                self._post("bridge", line)
                with self.lock:
                    if proc is not self.bridge_proc:
                        continue
                    if any(marker in line for marker in ("无法打开串口", "无法连接 VK", "无法连接 Valkey")):
                        self.last_bridge_error = line
                    if BRIDGE_READY_MARK in line:
                        self.bridge_ready.set()
                    if SESSION_READY_MARK in line:
                        self.session_ready = True
                        self._post("state")
        finally:
            with suppress(OSError):
                proc.stdout.close()
            code = proc.wait()
            with self.lock:
                if proc is self.bridge_proc:
                    self.bridge_exited.set()
                    self.session_ready = False
            self._post("bridge", "[控制台] 桥接进程已退出，exit=%s" % code,
                       "ok" if code == 0 else "error")
            self._post("bridge_exit", (proc, code))

    def _stop_worker(self, close_qmt):
        try:
            with self.lock:
                self._watchdog_active = False
                self._qmt_missing_since = None
            self._terminate_bridge(self.bridge_proc)
            self._stop_external_bridges()
            if close_qmt and self.config.qmt_root:
                if core.running_qmt_processes(self.config.qmt_root):
                    self._post("bridge", "[控制台] 关闭 QMT…")
                    code, _output = self._qmt_action("close", timeout=180)
                    if code != 0:
                        self._post("bridge", "[控制台] 关闭 QMT 失败，exit=%s。"
                                   "QMT 可能以管理员权限运行；请以管理员身份重启本程序。"
                                   % code, "warn")
                    self._wait_qmt_stopped(60)
            self._post("hint", "已停止。")
        finally:
            self._set_bridge_stopped()
            self._set_busy(False)

    def _restart_worker(self, close_qmt):
        try:
            with self.lock:
                self._watchdog_active = False
                self._qmt_missing_since = None
            self._terminate_bridge(self.bridge_proc)
            self._stop_external_bridges()
            if close_qmt and self.config.qmt_root:
                if core.running_qmt_processes(self.config.qmt_root):
                    self._post("bridge", "[控制台] 重启：先关闭 QMT…")
                    code, _output = self._qmt_action("close", timeout=180)
                    if code != 0:
                        self._post("bridge", "[控制台] 关闭 QMT 失败，exit=%s；继续重启桥接。"
                                   "QMT 可能以管理员权限运行，请以管理员身份重启本程序。"
                                   % code, "warn")
                    self._wait_qmt_stopped(60)
        finally:
            self._set_bridge_stopped()
            self._set_busy(False)
            self._post("request_start")

    def _terminate_bridge(self, proc):
        if proc is None:
            return
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(8)
        except (OSError, subprocess.TimeoutExpired):
            with suppress(OSError):
                proc.kill()
        with self.lock:
            self.session_ready = False
        self._post("state")

    def _set_bridge_stopped(self):
        self.bridge_state = "stopped"
        self._post("state")

    def _stop_external_bridges(self):
        """停止占用当前串口、但不是本窗口启动的桥接进程。"""
        try:
            found = core.find_bridge_processes(self.config.port)
        except Exception:
            found = []
        own_pid = self.bridge_proc.pid if self.bridge_proc is not None else None
        found = [item for item in found if item["pid"] != own_pid]
        if not found:
            return
        pids = [item["pid"] for item in found]
        self._post("bridge", "[控制台] 停止占用 %s 的其他桥接进程：%s"
                   % (self.config.port, pids), "warn")
        core.terminate_bridge_processes(found)
        time.sleep(0.4)

    def _wait_qmt_stopped(self, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not core.running_qmt_processes(self.config.qmt_root):
                return True
            time.sleep(0.5)
        return False

    def _qmt_action(self, action, extra=None, timeout=180):
        if not self.python_command:
            self.python_command, self.python_desc = core.resolve_python(
                self.config.python or None)
        command = core.qmt_launcher_command(
            self.python_command, action, self.config, extra)
        self._post("bridge", "[控制台] qmt_launcher %s" % action, "dim")
        code, output = core.run_capture(
            command, cwd=self.config.env_dir, env=core.child_env(self.config),
            timeout=timeout)
        for line in output.splitlines():
            if line.strip():
                self._post("bridge", "[qmt_launcher] " + line)
        return code, output

    # ------------------------------------------------------------ monitoring
    def _check_bridge_process(self):
        if self.closing:
            return
        self.root.after(BRIDGE_CHECK_INTERVAL_MS, self._check_bridge_process)
        with self.lock:
            if not self._bridge_watchdog_active or self.busy:
                return
            proc = self.bridge_proc
            if proc is not None and proc.poll() is None:
                return
            self.session_ready = False
            self.bridge_state = "starting"
            self._set_busy(True)
        self.hint_var.set("桥接已退出，正在自动重启…")
        self._post("bridge", "[监控] 检测到桥接进程已退出，正在自动重启…", "warn")
        threading.Thread(target=self._start_worker, kwargs={"bridge_only": True}, daemon=True).start()

    def _monitor_qmt_loop(self):
        hinted_external = False
        while not self._closing_event.is_set():
            procs = []
            if self.config.qmt_root:
                try:
                    procs = core.running_qmt_processes(self.config.qmt_root)
                except Exception:
                    procs = []
            main_name = self.config.qmt_exe.lower()
            mains = [item for item in procs if item[1].lower() == main_name]
            aux_count = len(procs) - len(mains)
            try:
                bridges = core.find_bridge_processes(self.config.port)
            except Exception:
                bridges = []
            own_pid = self.bridge_proc.pid if self.bridge_proc is not None else None
            external = [item for item in bridges if item["pid"] != own_pid]
            with self.lock:
                self.qmt_running = bool(mains)
                self.qmt_pids = [item[0] for item in mains]
                self.qmt_aux_count = aux_count
                self.external_bridges = external
            if external and not hinted_external:
                hinted_external = True
                pids = ",".join(str(item["pid"]) for item in external[:5])
                self._post("hint", "检测到已在运行的桥接进程（PID %s）占用 %s；"
                                   "点「启动」会先询问是否停止它，点「停止」可直接清理。"
                           % (pids, self.config.port))
            self._maybe_watchdog(bool(mains))
            self._post("state")
            self._closing_event.wait(2.0)

    def _qmt_log_loop(self):
        if not self.config.qmt_root:
            return
        tailer = core.LogTailer(initial_bytes=64 * 1024)
        current_path = ""
        while not self._closing_event.is_set():
            path = core.qmt_log_path(self.config.qmt_root)
            if path != current_path:
                rollover = bool(current_path)
                current_path = path
                lines = tailer.switch(path, from_start=rollover)
                self._post("qmt", "[控制台] tail %s" % path, "dim")
                for line in lines:
                    self._post("qmt", line)
            else:
                for line in tailer.poll():
                    self._post("qmt", line)
            self._closing_event.wait(0.4)
        tailer.close()

    def _on_bridge_exit(self, proc, code):
        with self.lock:
            if (proc is not self.bridge_proc or self.closing or self.busy
                    or not self._bridge_watchdog_active):
                return
            self._qmt_missing_since = None
        self._set_bridge_stopped()
        self.hint_var.set("桥接已退出（exit=%s），等待自动重启。" % code)

    # ---------------------------------------------------------------- close
    def _begin_close(self):
        with self.lock:
            self._bridge_watchdog_active = False
            self._watchdog_active = False
            self.closing = True
            self._closing_event.set()
            return self.bridge_proc

    def _on_close(self):
        running = self.bridge_proc is not None and self.bridge_proc.poll() is None
        close_qmt = bool(self.stop_qmt_on_stop.get())
        if running:
            suffix = "并关闭 QMT" if close_qmt else "（保留 QMT）"
            if not messagebox.askokcancel(
                    "退出", "桥接仍在运行。退出将停止桥接%s。继续？" % suffix):
                return
        proc = self._begin_close()
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(3)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    proc.kill()
                except OSError:
                    pass
        if close_qmt and self.config.qmt_root:
            self._spawn_qmt_close()
        self.root.destroy()

    def _spawn_qmt_close(self):
        try:
            python_command = self.python_command
            if not python_command:
                python_command, _desc = core.resolve_python(self.config.python or None)
            command = core.qmt_launcher_command(python_command, "close", self.config)
            subprocess.Popen(
                command, cwd=self.config.env_dir, env=core.child_env(self.config),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=core.CREATE_NO_WINDOW)
        except Exception:
            pass

    # ---------------------------------------------------------------- helpers
    def _display_command(self, command):
        text = subprocess.list2cmdline([str(part) for part in command])
        return text.replace(self.config.vk_url, core.redact_url(self.config.vk_url))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Big QMT 串口桥接控制台：启动/停止/重启桥接和 QMT，并 tail 日志。")
    parser.add_argument("--env-file", default="", help=".env 路径（默认自动查找）")
    parser.add_argument("--qmt-dir", default="", help="QMT 安装目录（默认读 .env）")
    parser.add_argument("--account", default="", help="资金账号")
    parser.add_argument("--vk-url", default="", help="VK 连接串")
    parser.add_argument("--port", default="", help="桥接侧串口，默认 COM11")
    parser.add_argument("--baudrate", type=int, default=None)
    parser.add_argument("--max-frame-bytes", type=int, default=None)
    parser.add_argument("--python", default="",
                        help="运行桥接 / qmt_launcher 的 Python（默认自动查找）")
    parser.add_argument("--qmt-mode", choices=("exe", "login"), default="")
    parser.add_argument("--qmt-exe", default="")
    parser.add_argument("--pubsub", action="store_const", const=True, default=None,
                        help="桥接使用 VK pub/sub（等价 --vk-pubsub）")
    parser.add_argument("--autostart", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--restart-qmt", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--keep-qmt", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-auto-restart", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--stop-bridge-pids", default="", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def build_overrides(args):
    return {
        "qmt_dir": args.qmt_dir or None,
        "account": args.account or None,
        "vk_url": args.vk_url or None,
        "port": args.port or None,
        "baudrate": args.baudrate,
        "max_frame_bytes": args.max_frame_bytes,
        "python": args.python or None,
        "qmt_mode": args.qmt_mode or None,
        "qmt_exe": args.qmt_exe or None,
        "pubsub": args.pubsub,
    }


def main(argv=None):
    args = parse_args(argv)
    if core.IS_WINDOWS:
        try:
            import ctypes
            try:
                ctypes.windll.shcore.SetProcessDpiAwareness(1)
            except Exception:
                ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass
    try:
        config = core.load_config(build_overrides(args), env_file=args.env_file or None)
    except Exception as exc:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("配置错误", str(exc))
        root.destroy()
        return 1
    pending_stop_pids = []
    for part in str(args.stop_bridge_pids or "").split(","):
        part = part.strip()
        if part.isdigit():
            pending_stop_pids.append(int(part))
    root = tk.Tk()
    SerialControllerApp(root, config, autostart=args.autostart,
                        pending_stop_pids=pending_stop_pids,
                        restart_qmt_on_start=args.restart_qmt,
                        stop_qmt_on_stop=not args.keep_qmt,
                        auto_restart_qmt=not args.no_auto_restart)
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
