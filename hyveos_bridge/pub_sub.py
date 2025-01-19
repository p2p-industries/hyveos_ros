import asyncio

from hyveos_msgs.msg import ReceivedPubsubMessage
from hyveos_msgs.srv import PubsubPublish, PubsubSubscription
from hyveos_sdk import ManagedStream, PubSubService
from hyveos_sdk.protocol.bridge_pb2 import PubSubRecvMessage
from rclpy.impl.rcutils_logger import RcutilsLogger
from rclpy.publisher import Publisher

from .bridge import Bridge, BridgeClient, prepare_data, service_callback


class Subscription:
    event: asyncio.Event
    task: asyncio.Task
    logger: RcutilsLogger

    def __init__(
        self,
        stream: ManagedStream[PubSubRecvMessage],
        publisher: Publisher,
        logger: RcutilsLogger
    ):
        self.event = asyncio.Event()
        self.task = asyncio.create_task(self.run(stream, publisher))
        self.logger = logger

    async def run(self, stream: ManagedStream[PubSubRecvMessage], publisher: Publisher):
        async with stream:
            iterator = stream.__aiter__()

            while True:
                data_task = asyncio.create_task(iterator.__anext__())
                event_task = asyncio.create_task(self.event.wait())

                done, _ = await asyncio.wait(
                    [data_task, event_task],
                    return_when=asyncio.FIRST_COMPLETED
                )

                if data_task in done:
                    request = data_task.result()

                    data = request.msg.data.data
                    topic = request.msg.topic.topic

                    self.logger.info(f'Received message {data} in topic {topic}')

                    received_msg = ReceivedPubsubMessage()
                    received_msg.propagation_source = request.propagation_source.peer_id
                    received_msg.source = request.source.peer_id
                    received_msg.topic = request.msg.topic.topic
                    received_msg.data = request.msg.data.data
                    received_msg.msg_id = request.msg_id.id

                    publisher.publish(received_msg)

                if event_task in done:
                    break

    async def cancel(self):
        self.event.set()
        await self.task


class PubsubClient(BridgeClient):
    logger: RcutilsLogger
    pub_sub: PubSubService
    subscriptions: dict[str, Subscription]
    subscriptions_lock: asyncio.Lock

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/pub_sub/{name}'

        self.received_messages_publisher = node.create_publisher(
            ReceivedPubsubMessage,
            namespaced('received_messages'),
            10
        )
        self.send_request_service = node.create_service(
            PubsubPublish,
            namespaced('publish'),
            self._publish_callback
        )
        self.subscribe_service = node.create_service(
            PubsubSubscription,
            namespaced('subscribe'),
            self._subscribe_callback
        )
        self.unsubscribe_service = node.create_service(
            PubsubSubscription,
            namespaced('unsubscribe'),
            self._unsubscribe_callback
        )

        self.logger = node.get_logger()
        self.pub_sub = node.connection.get_pub_sub_service()
        self.subscriptions = {}
        self.subscriptions_lock = asyncio.Lock()

    @service_callback
    async def _publish_callback(
        self,
        request: PubsubPublish.Request,
        response: PubsubPublish.Response
    ):
        self.logger.info(f'Publishing message to topic {request.topic}')

        data = prepare_data(request.data)

        msg_id = await self.pub_sub.publish(data, request.topic)

        response.success = True
        response.msg_id = msg_id
        return response

    @service_callback
    async def _subscribe_callback(
        self,
        request: PubsubSubscription.Request,
        response: PubsubSubscription.Response
    ):
        topic = request.topic
        self.logger.info(f'Subscribing to messages with topic {topic}')

        async with self.subscriptions_lock:
            if topic not in self.subscriptions:
                stream = await self.pub_sub.subscribe(topic)
                self.subscriptions[topic] = Subscription(
                    stream,
                    self.received_messages_publisher,
                    self.logger
                )
            else:
                raise ValueError('Already subscribed to topic')

        response.success = True
        return response

    @service_callback
    async def _unsubscribe_callback(
        self,
        request: PubsubSubscription.Request,
        response: PubsubSubscription.Response
    ):
        topic = request.topic
        self.logger.info(f'Unsubscribing from messages with topic {topic}')

        async with self.subscriptions_lock:
            if topic in self.subscriptions:
                await self.subscriptions.pop(topic).cancel()
            else:
                raise ValueError('Not subscribed to topic')

        response.success = True
        return response

    async def run(self):
        pass
