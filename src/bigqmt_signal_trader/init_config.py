# coding: utf-8
"""交互式生成桥接配置。

    bigqmt-init
    python -m bigqmt_signal_trader.init_config

可生成 QMT 端配置、客户端配置或内嵌配置的单文件策略（#56）。
配置文件会保存 Valkey 密码。QMT 登录密码由启动器从
BIGQMT_LOGIN_PASSWORD 环境变量读取。
"""

import getpass
import os
import subprocess
import tempfile
import sys

from .local_env import read_local_env


ACCOUNT_TYPES = ("STOCK", "CREDIT", "FUTURE", "STOCK_OPTION", "HUGANGTONG", "SHENGANGTONG")
TRANSPORTS = ("valkey",)
DEPLOYMENTS = ("package", "single_file")

DEPLOYMENT_LABELS = {
    "package": "标准包部署（把 src/ 同步到 QMT 的 python 目录）",
    "single_file": "单文件（base64 内嵌）",
}

BUILDERS = {
    "single_file": ("build_single_file.py", "BIGQMT_VALKEY_DRYRUN_ALL_IN_ONE.py", "utf-8"),
}

DEFAULTS = {
    "account_id": "",
    "account_type": "STOCK",
    "transport": "valkey",
    "host": "127.0.0.1",
    "port": 6379,
    "db": 5,
    "username": "",
    "password": "",
    "allow_order_methods": False,
    "deployment": "package",
    "qmt_python_dir": "",
    "client_dir": "",
}


# ----------------------------------------------------------------------
# 生成配置文本
# ----------------------------------------------------------------------

def _background_threads_for(transport):
    """各传输方式默认关闭后台线程，交易类方法始终在 adjust 线程执行。"""
    return False


def render_server_config(answers):
    """生成供 QMT 策略读取的配置。"""
    lines = [
        "# coding: utf-8",
        '"""QMT 策略的本地私有配置。',
        "",
        "由 bigqmt-init 生成。",
        "此文件含账号和 Valkey 凭据，请勿提交到版本库。",
        '"""',
        "",
        "BIGQMT_ACCOUNT_ID = %r" % str(answers["account_id"]),
        "",
        "BIGQMT_ACCOUNT_TYPE = %r" % str(answers["account_type"]),
        "",
        "BIGQMT_CONFIG = {",
        '    "transport": %r,' % str(answers["transport"]),
        '    "host": %r,' % str(answers["host"]),
        '    "port": %d,' % int(answers["port"]),
        '    "db": %d,' % int(answers["db"]),
        '    "username": %r,' % str(answers["username"]),
        '    "password": %r,' % str(answers["password"]),
    ]
    if answers["allow_order_methods"]:
        lines.append("    # 远程下单和撤单已开启。仅查询时请关闭。")
    else:
        lines.append("    # 默认关闭远程下单和撤单。")
    lines.extend([
        '    "rpc_allow_order_methods": %r,' % bool(answers["allow_order_methods"]),
        "    # get_trade_detail_data 在主策略线程外返回空结果，交易类方法始终由 adjust 执行（#244）。",
        "    # 各传输方式默认由 adjust 处理队列，后台线程每次获取 GIL 约需等一个调度周期（#343）。",
        '    "rpc_process_in_listener": True,',
        '    "rpc_listener_methods": ("*",),',
        '    "rpc_background_threads": %r,' % _background_threads_for(answers["transport"]),
        '    "schedule_adjust": True,',
        '    "schedule_adjust_interval": "100nMilliSecond",',
        '    "full_tick_cache_enabled": False,',
        '    "full_tick_demand_ttl_seconds": 10,',
        '    "full_tick_cache_ttl_seconds": 10,',
        '    "full_tick_refresh_interval_seconds": 0.5,',
        '    "full_tick_market_refresh_interval_seconds": 3,',
        '    "full_tick_refresh_max_wall_seconds": 0.3,',
        '    "full_tick_max_requests": 8,',
        '    "download_jobs_enabled": False,',
        '    "download_job_chunk_size": 10,',
        '    "download_job_max_wall_seconds": 0.5,',
        '    "download_job_ttl_seconds": 3600,',
        '    "exec_events_enabled": True,',
        '    "exec_events_debug_raw_fields": False,',
        "}",
        "",
    ])
    return "\n".join(lines)


def render_client_config(answers):
    """生成供外部客户端读取的配置。"""
    transport = str(answers["transport"])
    lines = [
        "# coding: utf-8",
        '"""客户端的本地私有配置。',
        "",
        "由 bigqmt-init 生成。",
        "此文件含账号和 Valkey 凭据，请勿提交到版本库。",
        '"""',
        "",
        "BIGQMT_ACCOUNT_ID = %r" % str(answers["account_id"]),
        "BIGQMT_RPC_TIMEOUT_SECONDS = 30.0",
        "",
        "BIGQMT_CONFIG = {",
        '    "transport": %r,' % transport,
        '    "host": %r,' % str(answers["host"]),
        '    "port": %d,' % int(answers["port"]),
        '    "db": %d,' % int(answers["db"]),
        '    "username": %r,' % str(answers["username"]),
        '    "password": %r,' % str(answers["password"]),
    ]
    lines.extend([
        "}",
        "",
        "# 参考数据和历史数据优先从 FormulaServer（58600 端口）读取，无法读取时回退到 RPC。",
        "BIGQMT_FORMULA_SERVER_CONFIG = {",
        '    "enabled": True,',
        "}",
        "",
        "BIGQMT_LOCAL_CACHE_CONFIG = {",
        '    "enabled": True,',
        '    "dir": None,',
        '    "fallback_rpc": True,  # 兼容 MiniQMT 的复权数据读取，设为 False 时仅读本地缓存。',
        '    "format": "auto",',
        "}",
        "",
    ])
    return "\n".join(lines)


def render_single_file_config_block(answers):
    """生成单文件顶部的配置块，运行时据此在内存中生成配置模块（#153）。"""
    transport = str(answers["transport"])
    lines = [
        "BIGQMT_ACCOUNT_ID = %r" % str(answers["account_id"]),
        "",
        "BIGQMT_ACCOUNT_TYPE = %r" % str(answers["account_type"]),
        "",
        "BIGQMT_CONFIG = {",
        '    "transport": %r,' % transport,
        '    "host": %r,' % str(answers["host"]),
        '    "port": %d,' % int(answers["port"]),
        '    "db": %d,' % int(answers["db"]),
        '    "username": %r,' % str(answers["username"]),
        '    "password": %r,' % str(answers["password"]),
    ]
    lines.extend([
        '    "rpc_allow_order_methods": %r,' % bool(answers["allow_order_methods"]),
        '    "rpc_process_in_listener": True,',
        '    "rpc_listener_methods": ("*",),',
        '    "rpc_background_threads": %r,' % _background_threads_for(answers["transport"]),
        '    "schedule_adjust": True,',
        '    "schedule_adjust_interval": "100nMilliSecond",',
        '    "full_tick_cache_enabled": False,',
        '    "full_tick_demand_ttl_seconds": 10,',
        '    "full_tick_cache_ttl_seconds": 10,',
        '    "full_tick_refresh_interval_seconds": 0.5,',
        '    "full_tick_market_refresh_interval_seconds": 3,',
        '    "full_tick_refresh_max_wall_seconds": 0.3,',
        '    "full_tick_max_requests": 8,',
        '    "download_jobs_enabled": False,',
        '    "download_job_chunk_size": 10,',
        '    "download_job_max_wall_seconds": 0.5,',
        '    "download_job_ttl_seconds": 3600,',
        '    "exec_events_enabled": True,',
        '    "exec_events_debug_raw_fields": False,',
        "}",
    ])
    return "\n".join(lines)


def inject_single_file_config(source, answers):
    """替换构建产物的占位配置，找不到配置块时抛出异常。"""
    start = source.find("BIGQMT_ACCOUNT_ID = ")
    if start < 0:
        raise ValueError("生成的单文件策略中找不到配置块")
    marker = "\n}\n"
    end = source.find(marker, start)
    if end < 0:
        raise ValueError("生成的单文件策略缺少配置块的结束标记")
    return source[:start] + render_single_file_config_block(answers) + source[end + len(marker) - 1:]


# ----------------------------------------------------------------------
# 交互提示
# ----------------------------------------------------------------------

def _ask(write, read, question, default="", choices=None, secret=False,
         read_secret=None):
    while True:
        if choices:
            write("\n%s\n" % question)
            for index, choice in enumerate(choices, 1):
                label = DEPLOYMENT_LABELS.get(choice, choice)
                marker = " (默认)" if choice == default else ""
                write("  %d) %s%s\n" % (index, label, marker))
            raw = read("请选择 [1-%d]: " % len(choices)).strip()
            if not raw:
                return default
            if raw.isdigit() and 1 <= int(raw) <= len(choices):
                return choices[int(raw) - 1]
            if raw in choices:
                return raw
            write("  无效选择，请重试。\n")
            continue

        suffix = ""
        if default != "":
            suffix = " [已配置]" if secret else " [%s]" % default
        if secret:
            ask_secret = read_secret or getpass.getpass
            raw = ask_secret("%s%s: " % (question, suffix))
        else:
            raw = read("%s%s: " % (question, suffix))
        raw = raw.strip()
        return raw if raw else default


def _ask_bool(read, question, default=False):
    hint = "Y/n" if default else "y/N"
    raw = read("%s [%s]: " % (question, hint)).strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes", "是", "1", "true")


def _ask_int(write, read, question, default):
    while True:
        raw = _ask(write, read, question, str(default))
        try:
            return int(raw)
        except ValueError:
            write("  需要一个整数。\n")


def prompt_answers(write, read, read_secret=None):
    answers = dict(DEFAULTS)
    local_env = read_local_env()

    write("\n=== Big QMT 桥接配置 ===\n")

    while not answers["account_id"]:
        answers["account_id"] = _ask(
            write, read, "资金账号", local_env.get("BIGQMT_ACCOUNT_ID", "").strip())
        if not answers["account_id"]:
            write("  账号不能为空。\n")

    account_type = local_env.get("BIGQMT_ACCOUNT_TYPE", "").strip().upper()
    answers["account_type"] = _ask(
        write, read, "账号类型",
        default=account_type if account_type in ACCOUNT_TYPES else "STOCK", choices=ACCOUNT_TYPES)

    answers["transport"] = _ask(
        write, read, "传输方式", default="valkey", choices=TRANSPORTS)

    if answers["transport"] == "valkey":
        answers["host"] = _ask(write, read, "Valkey 地址",
                               local_env.get("BIGQMT_VALKEY_HOST") or DEFAULTS["host"])
        answers["port"] = _ask_int(write, read, "Valkey 端口",
                                   local_env.get("BIGQMT_VALKEY_PORT") or DEFAULTS["port"])
        answers["db"] = _ask_int(write, read, "Valkey 数据库编号",
                                 local_env.get("BIGQMT_VALKEY_DB") or DEFAULTS["db"])
        answers["username"] = _ask(write, read, "Valkey 用户名（无则回车）",
                                   local_env.get("BIGQMT_VALKEY_USERNAME", ""))
        answers["password"] = _ask(
            write, read, "Valkey 密码（输入不回显）", local_env.get("BIGQMT_VALKEY_PASSWORD", ""),
            secret=True, read_secret=read_secret)

    write("\n远程下单/撤单默认关闭。打开后，任何能连上这条通道的程序都可以下单。\n")
    answers["allow_order_methods"] = _ask_bool(
        read, "允许远程下单/撤单？", default=False)

    answers["deployment"] = _ask(
        write, read, "部署方式", default="package", choices=DEPLOYMENTS)

    answers["qmt_python_dir"] = _ask(
        write, read, "QMT 的 python 目录（回车则写到当前目录）", "")
    answers["client_dir"] = _ask(
        write, read, "客户端配置写到哪个目录（回车则当前目录）", "")
    return answers


# ----------------------------------------------------------------------
# 写入配置
# ----------------------------------------------------------------------

def _confirm_overwrite(write, read, path, force):
    if not os.path.exists(path) or force:
        return True
    return _ask_bool(read, "%s 已存在，覆盖？" % path, default=False)


def _write(path, text, encoding="utf-8"):
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, "w", encoding=encoding, newline="\n") as handle:
        handle.write(text)
    # 在支持 chmod 的平台上仅给所有者读写权限。Windows 的 NTFS 权限另行设置。
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _materialize_packaged_builder(script):
    """把安装包附带的构建脚本解包到临时目录，缺少脚本时返回 None。"""
    try:
        import pkgutil
        data = pkgutil.get_data(
            "bigqmt_signal_trader", "_singlefile/%s.txt" % script)
    except Exception:
        data = None
    if not data:
        return None
    tmpdir = tempfile.mkdtemp(prefix="bigqmt-builder-")
    # 同时解包所选构建脚本及其可能依赖的 Valkey 构建脚本。
    for name in {script, BUILDERS["single_file"][0]}:
        payload = pkgutil.get_data(
            "bigqmt_signal_trader", "_singlefile/%s.txt" % name)
        if payload:
            with open(os.path.join(tmpdir, name), "wb") as handle:
                handle.write(payload)
    return os.path.join(tmpdir, script)


def build_single_file(repo_root, deployment, answers, out_path=None):
    """运行构建器并把向导配置写入产物。"""
    script, default_name, encoding = BUILDERS[deployment]
    builder = os.path.join(repo_root, "tools", script)
    if not os.path.exists(builder):
        builder = _materialize_packaged_builder(script)
    if builder is None:
        raise ValueError(
            "找不到构建器 %s（已检查 %s 和安装包内的 "
            "bigqmt_signal_trader/_singlefile）。请升级 xtquant-big-convert，"
            "或通过 --repo-root 指定源码仓库。" % (script, os.path.join(repo_root, "tools", script)))
    target = out_path or os.path.join(os.getcwd(), default_name)
    env = dict(os.environ, BIGQMT_BUILD_OUT=target, PYTHONIOENCODING="utf-8")
    try:
        import bigqmt_signal_trader as _pkg
        package_parent = os.path.dirname(os.path.dirname(os.path.abspath(_pkg.__file__)))
    except Exception:
        package_parent = ""
    if package_parent:
        # 让构建子进程从当前安装位置找到要嵌入的源码。
        env["PYTHONPATH"] = os.pathsep.join(
            part for part in (package_parent, os.environ.get("PYTHONPATH", ""))
            if part)
    completed = subprocess.run(
        [sys.executable, builder], env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if completed.returncode != 0:
        raise ValueError("%s 构建失败：\n%s"
                         % (script, completed.stdout.decode("utf-8", "replace")))
    with open(target, encoding=encoding) as handle:
        source = handle.read()
    with open(target, "w", encoding=encoding, newline="\n") as handle:
        handle.write(inject_single_file_config(source, answers))
    return target


def apply(answers, repo_root, write, read, force=False):
    """Write everything the answers call for. Returns the paths written."""
    written = []

    server_dir = answers["qmt_python_dir"] or os.getcwd()
    server_path = os.path.join(server_dir, "bigqmt_signal_trader_qmt_config.py")
    if _confirm_overwrite(write, read, server_path, force):
        _write(server_path, render_server_config(answers))
        written.append(server_path)

    client_dir = answers["client_dir"] or os.getcwd()
    client_path = os.path.join(client_dir, "bigqmt_signal_trader_client_config.py")
    if _confirm_overwrite(write, read, client_path, force):
        _write(client_path, render_client_config(answers))
        written.append(client_path)

    if answers["deployment"] in BUILDERS:
        _script, default_name, _encoding = BUILDERS[answers["deployment"]]
        target = os.path.join(server_dir, default_name)
        if _confirm_overwrite(write, read, target, force):
            written.append(build_single_file(
                repo_root, answers["deployment"], answers, out_path=target))

    return written


def _report(write, answers, written):
    write("\n=== 已写入 ===\n")
    for path in written:
        write("  %s\n" % path)
    if not written:
        write("  （没有文件被写入）\n")
        return

    write("\n这些文件包含账号和凭据，不要提交到版本库。\n")

    if answers["deployment"] == "package":
        write("\n下一步：把 src/ 下的包同步到 QMT 的 python 目录，"
              "在策略编辑器里加载 BIGQMT_VALKEY_DRYRUN.py。\n")
    else:
        write("\n下一步：把生成的单文件拷进 QMT 的 python 目录并作为策略加载，"
              "不需要一并拷贝整个包。\n")

    if answers["allow_order_methods"]:
        write("\n注意：远程下单/撤单已打开。任何能连上这条通道的程序都可以下单。\n")

    write("\nqmt_launcher 的自动登录密码不写进配置文件——"
          "它从环境变量 BIGQMT_LOGIN_PASSWORD 读取，\n"
          "这样密码不会出现在 argv 或磁盘文件里。需要时先设置该环境变量。\n")


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(
        prog="bigqmt-init",
        description="交互式生成 Big QMT 桥接的服务端/客户端配置。")
    parser.add_argument("--force", action="store_true",
                        help="已存在的文件直接覆盖，不再询问")
    parser.add_argument("--repo-root", default=None,
                        help="仓库根目录（单文件构建需要 tools/ 下的生成器）")
    args = parser.parse_args(argv)

    repo_root = args.repo_root or os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    write = sys.stdout.write
    read = lambda prompt: input(prompt)  # noqa: E731 -- injected in tests

    try:
        answers = prompt_answers(write, read)
        written = apply(answers, repo_root, write, read, force=args.force)
    except KeyboardInterrupt:
        write("\n已取消，未写入任何文件。\n")
        return 1
    except ValueError as exc:
        write("\n失败：%s\n" % exc)
        return 1

    _report(write, answers, written)
    return 0


if __name__ == "__main__":
    sys.exit(main())
