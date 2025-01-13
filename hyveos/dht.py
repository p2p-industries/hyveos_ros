from hyveos_msgs.srv import DHTGetRecord, DHTPutRecord
from hyveos_sdk import DHTService
from rclpy.impl.rcutils_logger import RcutilsLogger

from .bridge import Bridge, BridgeClient, service_callback


class DHTClient(BridgeClient):
    logger: RcutilsLogger
    dht: DHTService

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/dht/{name}'

        self.get_record_service = node.create_service(
            DHTGetRecord,
            namespaced('get_record'),
            self._get_record_callback
        )
        self.put_record_service = node.create_service(
            DHTPutRecord,
            namespaced('put_record'),
            self._put_record_callback
        )

        self.logger = node.get_logger()
        self.dht = node.connection.get_dht_service()

    @service_callback
    async def _get_record_callback(
        self,
        request: DHTGetRecord.Request,
        response: DHTGetRecord.Response
    ):
        self.logger.info(f'Getting record in topic {request.topic} with key {request.key}')

        record = await self.dht.get_record(request.topic, request.key)

        if record.data is None:
            response.success = False
            response.error = 'Record not found'
        else:
            response.success = True
            response.data = record.data.data

        return response

    @service_callback
    async def _put_record_callback(
        self,
        request: DHTPutRecord.Request,
        response: DHTPutRecord.Response
    ):
        self.logger.info(f'Putting record in topic {request.topic} with key {request.key}')

        await self.dht.put_record(request.topic, request.key, request.value)

        response.success = True
        return response

    async def run(self):
        pass
