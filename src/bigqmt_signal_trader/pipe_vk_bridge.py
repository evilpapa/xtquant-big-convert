"""Windows 外部桥接：在本机命名管道与 VK 之间转发大 QMT RPC。"""

import logging

from .serial_vk_bridge import SerialVkBridge, _bridge_main
from .transports.pipe_transport import DEFAULT_PIPE_NAME, local_pipe_name


_log = logging.getLogger(__name__)


class PipeVkBridge(SerialVkBridge):
    """用 Windows 命名管道连接 QMT；断线后等待新的本机客户端。"""

    link_label = "命名管道"
    link_name = "Named pipe"
    cli_arg = "--pipe-name"
    cli_default = DEFAULT_PIPE_NAME
    vk_env_var = "BIGQMT_PIPE_VK_URL"

    @staticmethod
    def normalize_endpoint(name):
        return local_pipe_name(name)

    @staticmethod
    def open_listener(name):
        from .transports._win_pipe import PipeListener

        return PipeListener(name)

    def step(self, pubsub=None):
        from .transports._win_pipe import PipeDisconnected

        port = self.wire.port
        if not port.accept():
            return False
        try:
            return super().step(pubsub)
        except PipeDisconnected:
            port.disconnect()
            self._reset_link()
            _log.warning("QMT 命名管道已断开，等待策略重新连接；已转发的请求不会重放")
            return True


def main(argv=None):
    return _bridge_main(argv, PipeVkBridge)


if __name__ == "__main__":
    raise SystemExit(main())
