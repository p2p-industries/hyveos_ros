from hyveos_msgs.srv import GetKVRecord, PutKVRecord
from hyveos_sdk import KVService
from rclpy.impl.rcutils_logger import RcutilsLogger

from .bridge import Bridge, BridgeClient, service_callback


class KVClient(BridgeClient):
    logger: RcutilsLogger
    kv: KVService

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/kv/{name}'

        self.get_record_service = node.create_service(
            GetKVRecord,
            namespaced('get_record'),
            self._get_record_callback
        )
        self.put_record_service = node.create_service(
            PutKVRecord,
            namespaced('put_record'),
            self._put_record_callback
        )

        self.logger = node.get_logger()
        self.kv = node.connection.get_kv_service()

    @service_callback
    async def _get_record_callback(
        self,
        request: GetKVRecord.Request,
        response: GetKVRecord.Response
    ):
        self.logger.info(f'Getting record in topic {request.topic} with key {request.key}')

        record = await self.kv.get_record(request.topic, request.key)

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
        request: PutKVRecord.Request,
        response: PutKVRecord.Response
    ):
        self.logger.info(f'Putting record in topic {request.topic} with key {request.key}')

        await self.kv.put_record(request.topic, request.key, request.value)

        response.success = True
        return response

    async def run(self):
        pass
