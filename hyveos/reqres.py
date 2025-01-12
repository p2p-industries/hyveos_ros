import asyncio
import rclpy

from hyveos_msgs.msg import ReceivedRequest
from hyveos_msgs.srv import RequestSubscription, Respond, SendRequest
from hyveos_sdk import ManagedStream, RequestResponseService
from hyveos_sdk.protocol.script_pb2 import RecvRequest
from rclpy.publisher import Publisher
from rclpy.impl.rcutils_logger import RcutilsLogger

from .bridge import BridgeClient, Bridge, prepare_data, service_callback

class Subscription:
    event: asyncio.Event
    task: asyncio.Task
    logger: RcutilsLogger

    def __init__(self, stream: ManagedStream[RecvRequest], publisher: Publisher, logger: RcutilsLogger):
        self.event = asyncio.Event()
        self.task = asyncio.create_task(self.run(stream, publisher))
        self.logger = logger

    async def run(self, stream: ManagedStream[RecvRequest], publisher: Publisher):
        async with stream:
            iterator = stream.__aiter__()

            while True:
                data_task = asyncio.create_task(iterator.__anext__())
                event_task = asyncio.create_task(self.event.wait())

                done, _ = await asyncio.wait([data_task, event_task], return_when=asyncio.FIRST_COMPLETED)

                if data_task in done:
                    request = data_task.result()

                    self.logger.info(f'Received request {request.seq} from {request.peer.peer_id}')

                    request_msg = ReceivedRequest()
                    request_msg.peer = request.peer.peer_id
                    if request.msg.topic.topic is None:
                        request_msg.topic = ''
                        request_msg.no_topic = True
                    else:
                        request_msg.topic = request.msg.topic.topic.topic
                        request_msg.no_topic = False
                    request_msg.data = request.msg.data.data
                    request_msg.seq = request.seq

                    publisher.publish(request_msg)

                if event_task in done:
                    break

    async def cancel(self):
        self.event.set()
        await self.task

class ReqResClient(BridgeClient):
    logger: RcutilsLogger
    req_res: RequestResponseService
    subscriptions: dict[str | None, Subscription]
    subscriptions_lock: asyncio.Lock

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/req_res/{name}'

        self.received_requests_publisher = node.create_publisher(ReceivedRequest, namespaced('received_requests'), 10)
        self.send_request_service = node.create_service(SendRequest, namespaced('send_request'), self._send_request_callback)
        self.subscribe_service = node.create_service(RequestSubscription, namespaced('subscribe'), self._subscribe_callback)
        self.unsubscribe_service = node.create_service(RequestSubscription, namespaced('unsubscribe'), self._unsubscribe_callback)
        self.respond_service = node.create_service(Respond, namespaced('respond'), self._respond_callback)

        self.logger = node.get_logger()
        self.req_res = node.connection.get_request_response_service()
        self.subscriptions = {}
        self.subscriptions_lock = asyncio.Lock()

    @service_callback
    async def _send_request_callback(self, request: SendRequest.Request, response: SendRequest.Response):
        if request.no_topic:
            self.logger.info(f'Sending request without topic to {request.peer}')
            topic = None
        else:
            self.logger.info(f'Sending request with topic {request.topic} to {request.peer}')
            topic = request.topic

        data = prepare_data(request.data)

        res = await self.req_res.send_request(request.peer, data, topic=topic)

        if res.WhichOneof('response') == 'data':
            response.success = True
            response.response = res.data.data
            return response
        elif res.WhichOneof('response') == 'error':
            raise ValueError(res.error)
        else:
            raise ValueError('Invalid response')

    @service_callback
    async def _subscribe_callback(self, request: RequestSubscription.Request, response: RequestSubscription.Response):
        if request.no_topic:
            self.logger.info('Subscribing to messages without topic')
            topic = None
        else:
            self.logger.info(f'Subscribing to messages with topic {request.topic}')
            topic = request.topic

        async with self.subscriptions_lock:
            if topic not in self.subscriptions:
                stream = self.req_res.receive(query=topic)
                self.subscriptions[topic] = Subscription(stream, self.received_requests_publisher, self.logger)
            else:
                raise ValueError('Already subscribed to topic')

        response.success = True
        return response

    @service_callback
    async def _unsubscribe_callback(self, request: RequestSubscription.Request, response: RequestSubscription.Response):
        if request.no_topic:
            self.logger.info('Unsubscribing from messages without topic')
            topic = None
        else:
            self.logger.info(f'Unsubscribing from messages with topic {request.topic}')
            topic = request.topic

        async with self.subscriptions_lock:
            if topic in self.subscriptions:
                await self.subscriptions.pop(topic).cancel()
            else:
                raise ValueError('Not subscribed to topic')

        response.success = True
        return response

    @service_callback
    async def _respond_callback(self, request: Respond.Request, response: Respond.Response):
        self.logger.info(f'Responding to request {request.seq}')

        if request.success:
            data = prepare_data(request.response)
            await self.req_res.respond(request.seq, data)
        else:
            await self.req_res.respond(request.seq, b'', error=request.error)

        response.success = True
        return response

    async def run(self):
        pass
