import asyncio
import rclpy

from hyveos_msgs.msg import ReceivedRequest
from hyveos_msgs.srv import RequestSubscription, Respond, SendRequest
from hyveos_sdk import ManagedStream, RequestResponseService
from hyveos_sdk.protocol.script_pb2 import RecvRequest
from rclpy.publisher import Publisher

from .bridge import BridgeClient, Bridge, service_callback

class Subscription:
    event: asyncio.Event
    task: asyncio.Task

    def __init__(self, stream: ManagedStream[RecvRequest], publisher: Publisher):
        self.event = asyncio.Event()
        self.task = asyncio.create_task(self.run(stream, publisher))

    async def run(self, stream: ManagedStream[RecvRequest], publisher: Publisher):
        async with stream:
            while True:
                data_task = asyncio.create_task(stream.__anext__())
                event_task = asyncio.create_task(self.event.wait())

                done, _ = await asyncio.wait([data_task, event_task], return_when=asyncio.FIRST_COMPLETED)

                if data_task in done:
                    request = data_task.result()
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
    req_res: RequestResponseService
    subscriptions: dict[str | None, Subscription]
    subscriptions_lock: asyncio.Lock

    def __init__(self, node: Bridge):
        self.received_requests_publisher = node.create_publisher(ReceivedRequest, '/reqres/received_requests', 10)
        self.send_request_service = node.create_service(SendRequest, '/reqres/send_request', self._send_request_callback)
        self.subscribe_service = node.create_service(RequestSubscription, '/reqres/subscribe', self._subscribe_callback)
        self.unsubscribe_service = node.create_service(RequestSubscription, '/reqres/unsubscribe', self._unsubscribe_callback)
        self.respond_service = node.create_service(Respond, '/reqres/respond', self._respond_callback)

        self.req_res = node.connection.get_request_response_service()
        self.subscriptions = {}
        self.subscriptions_lock = asyncio.Lock()

    @service_callback
    async def _send_request_callback(self, request: SendRequest.Request, response: SendRequest.Response):
        topic = None if request.no_topic else request.topic

        res = await self.req_res.send_request(request.peer, request.data, topic=topic)

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
        topic = None if request.no_topic else request.topic

        async with self.subscriptions_lock:
            if topic not in self.subscriptions:
                stream = self.req_res.receive(query=topic)
                self.subscriptions[topic] = Subscription(stream, self.received_requests_publisher)
            else:
                raise ValueError('Already subscribed to topic')

        response.success = True
        return response

    @service_callback
    async def _unsubscribe_callback(self, request: RequestSubscription.Request, response: RequestSubscription.Response):
        topic = None if request.no_topic else request.topic

        async with self.subscriptions_lock:
            if topic in self.subscriptions:
                await self.subscriptions.pop(topic).cancel()
            else:
                raise ValueError('Not subscribed to topic')

        response.success = True
        return response

    @service_callback
    async def _respond_callback(self, request: Respond.Request, response: Respond.Response):
        if request.success:
            await self.req_res.respond(request.seq, request.response)
        else:
            await self.req_res.respond(request.seq, b'', error=request.error)

        response.success = True
        return response

    async def run(self):
        pass
