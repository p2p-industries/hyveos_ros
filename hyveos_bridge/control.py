from hyveos_msgs.srv import GetId
from hyveos_sdk import OpenedConnection
from rclpy.impl.rcutils_logger import RcutilsLogger

from .bridge import Bridge, BridgeClient, service_callback


class ControlClient(BridgeClient):
    logger: RcutilsLogger
    connection: OpenedConnection

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/{name}'

        self.get_id_service = node.create_service(
            GetId,
            namespaced('get_id'),
            self._get_id_callback
        )

        self.logger = node.get_logger()
        self.connection = node.connection

    @service_callback
    async def _get_id_callback(
        self,
        _: GetId.Request,
        response: GetId.Response
    ):
        self.logger.info('Getting own ID')

        response.id = await self.connection.get_id()
        response.success = True
        return response

    async def run(self):
        pass
