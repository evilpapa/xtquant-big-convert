"""Windows 外部桥接：在本机 WM_COPYDATA 隐藏窗口与 VK 之间转发大 QMT RPC。"""

import logging

from .serial_vk_bridge import SerialVkBridge, _bridge_main
from .transports.copydata_transport import DEFAULT_WINDOW_NAME, local_window_title


_log = logging.getLogger(__name__)


class CopyDataVkBridge(SerialVkBridge):
    """用 Windows 隐藏窗口（WM_COPYDATA）连接 QMT；断线后等待新的本机客户端。"""

    link_label = "窗口消息"
    link_name = "WM_COPYDATA"
    cli_arg = "--window-name"
    cli_default = DEFAULT_WINDOW_NAME
    vk_env_var = "BIGQMT_COPYDATA_VK_URL"

    @staticmethod
    def normalize_endpoint(name):
        return local_window_title(name)

    @staticmethod
    def open_listener(name):
        from .transports._win_copydata import CopyDataListener

        return CopyDataListener(name)

    def step(self, pubsub=None):
        from .transports._win_copydata import CopyDataDisconnected

        port = self.wire.port
        if not port.accept():
            return False
        try:
            return super().step(pubsub)
        except CopyDataDisconnected:
            port.disconnect()
            self._reset_link()
            _log.warning("QMT 窗口消息连接已断开，等待策略重新连接；已转发的请求不会重放")
            return True


def main(argv=None):
    return _bridge_main(argv, CopyDataVkBridge)


if __name__ == "__main__":
    raise SystemExit(main())
