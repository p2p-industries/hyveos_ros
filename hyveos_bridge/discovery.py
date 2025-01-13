import asyncio

from hyveos_msgs.msg import NeighbourEvent
from hyveos_msgs.srv import GetId, GetNeighbours
from hyveos_sdk import DiscoveryService
from rclpy.impl.rcutils_logger import RcutilsLogger

from .bridge import Bridge, BridgeClient, service_callback


class DiscoveryClient(BridgeClient):
    logger: RcutilsLogger
    discovery: DiscoveryService
    neighbours: set[str]
    neighbours_lock: asyncio.Lock

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/{name}'

        self.neighbour_events_publisher = node.create_publisher(
            NeighbourEvent,
            namespaced('neighbour_events'),
            10
        )
        self.get_id_service = node.create_service(
            GetId,
            namespaced('get_id'),
            self._get_id_callback
        )
        self.get_neighbours_service = node.create_service(
            GetNeighbours,
            namespaced('get_neighbours'),
            self._get_neighbours_callback
        )

        self.logger = node.get_logger()
        self.discovery = node.connection.get_discovery_service()
        self.neighbours = set()
        self.neighbours_lock = asyncio.Lock()

    @service_callback
    async def _get_id_callback(
        self,
        _: GetId.Request,
        response: GetId.Response
    ):
        self.logger.info('Getting own ID')

        response.id = await self.discovery.get_own_id()
        response.success = True
        return response

    @service_callback
    async def _get_neighbours_callback(
        self,
        _: GetNeighbours.Request,
        response: GetNeighbours.Response
    ):
        self.logger.info('Getting neighbours')

        async with self.neighbours_lock:
            response.success = True
            response.neighbour_ids = list(self.neighbours)
            return response

    async def run(self):
        async with self.discovery.discovery_events() as events:
            async for event in events:
                event_type = event.WhichOneof('event')
                if event_type == 'init':
                    async with self.neighbours_lock:
                        self.neighbours = {peer.peer_id for peer in event.init.peers}
                elif event_type == 'discovered':
                    peer_id = event.discovered.peer_id

                    async with self.neighbours_lock:
                        self.neighbours.add(peer_id)

                    msg = NeighbourEvent()
                    msg.event = NeighbourEvent.DISCOVERED
                    msg.neighbour_id = peer_id
                    self.neighbour_events_publisher.publish(msg)
                elif event_type == 'lost':
                    peer_id = event.lost.peer_id

                    async with self.neighbours_lock:
                        self.neighbours.discard(peer_id)

                    msg = NeighbourEvent()
                    msg.event = NeighbourEvent.LOST
                    msg.neighbour_id = peer_id
                    self.neighbour_events_publisher.publish(msg)
                else:
                    self.logger.warn(f'Unknown event type: {event_type}')
