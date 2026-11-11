# -*- coding: utf-8 -*-
"""Build the self-contained named-pipe strategy."""
import os

import build_single_file as bsf
import build_no_redis_single_file_flat as flat

ROOT = flat.ROOT
OUT_PATH = os.environ.get(
    "BIGQMT_BUILD_OUT", os.path.join(ROOT, "src", "BIGQMT_DRYRUN_PIPE_FLAT_ALL_IN_ONE.py"))

PIPE_TEMPLATE = flat.FLAT_TEMPLATE

def main():
    top_dir, package_dir = bsf.resolve_source_dirs()
    sources = bsf.collect_package(package_dir, top_dir)

    extra = {}
    for name in ("bigqmt_signal_trader_strategy.py",
                 "bigqmt_signal_trader_redis_rpc_runtime.py"):
        full = os.path.join(top_dir, name)
        with open(full, "rb") as f:
            extra[name] = f.read()

    funcs_block = flat.module_funcs_block(sources, extra)
    template = PIPE_TEMPLATE.replace("__MODULE_FUNCS_BLOCK__", funcs_block)

    total = sum(len(v) for v in sources.values()) + sum(len(v) for v in extra.values())
    with open(OUT_PATH, "w", encoding="gbk", newline="\n") as f:
        f.write(template)
    print("WROTE %s" % OUT_PATH)
    print("embedded files: %d package + %d top-level" % (len(sources), len(extra)))
    print("embedded raw bytes: %d (flat real code, not string/base64)" % total)


if __name__ == "__main__":
    main()
