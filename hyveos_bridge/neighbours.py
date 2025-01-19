from hyveos_msgs.msg import NeighbourEvent
from hyveos_msgs.srv import GetNeighbours
from hyveos_sdk import NeighboursService
from rclpy.impl.rcutils_logger import RcutilsLogger

from .bridge import Bridge, BridgeClient, service_callback


class NeighboursClient(BridgeClient):
    logger: RcutilsLogger
    neighbours: NeighboursService

    def __init__(self, node: Bridge):
        def namespaced(name: str) -> str:
            return f'{node.get_name()}/{name}'

        self.neighbour_events_publisher = node.create_publisher(
            NeighbourEvent,
            namespaced('neighbour_events'),
            10
        )
        self.get_neighbours_service = node.create_service(
            GetNeighbours,
            namespaced('get_neighbours'),
            self._get_neighbours_callback
        )

        self.logger = node.get_logger()
        self.neighbours = node.connection.get_neighbours_service()

    @service_callback
    async def _get_neighbours_callback(
        self,
        _: GetNeighbours.Request,
        response: GetNeighbours.Response
    ):
        self.logger.info('Getting neighbours')

        response.success = True
        response.neighbour_ids = await self.neighbours.get()
        return response

    async def run(self):
        async with self.neighbours.subscribe() as events:
            async for event in events:
                event_type = event.WhichOneof('event')
                if event_type == 'init':
                    self.logger.info('Neighbours initialized')
                elif event_type == 'discovered':
                    msg = NeighbourEvent()
                    msg.event = NeighbourEvent.DISCOVERED
                    msg.neighbour_id = event.discovered.peer_id
                    self.neighbour_events_publisher.publish(msg)
                elif event_type == 'lost':
                    msg = NeighbourEvent()
                    msg.event = NeighbourEvent.LOST
                    msg.neighbour_id = event.lost.peer_id
                    self.neighbour_events_publisher.publish(msg)
                else:
                    self.logger.warn(f'Unknown event type: {event_type}')
