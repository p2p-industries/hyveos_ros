import asyncio
import rclpy
from rclpy.node import Node

from hyveos_sdk import Connection, OpenedConnection

from abc import ABC, abstractmethod
from pathlib import Path

def service_callback(f):
    async def wrapper(self, request, response):
        try:
            return await f(self, request, response)
        except Exception as e:
            response.success = False
            response.error = str(e)
            return response
    return wrapper

class BridgeClient(ABC):
    @abstractmethod
    def __init__(self, node: 'Bridge'):
        pass

    @abstractmethod
    async def run(self):
        pass

class Bridge(Node):
    connection: OpenedConnection
    clients: list[BridgeClient]

    def __init__(self, connection: OpenedConnection):
        super().__init__('hyveos_bridge')

        self.connection = connection
        self.clients = [client(self) for client in BridgeClient.__subclasses__()]

    async def run(self):
        coroutines = [client.run() for client in self.clients]
        await asyncio.gather(*coroutines)

async def ros_loop(node: Node):
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0)
        await asyncio.sleep(1e-4)

async def async_main(args=None):
    def find_bridge_path(name: str) -> Path:
        candidates = ['/run', '/var/run', '/tmp']

        for candidate in candidates:
            path = Path(candidate) / 'hyved' / 'bridge' / name
            if path.exists():
                return path

        raise FileNotFoundError(f'Bridge {name} not found')

    socket_path = find_bridge_path('bridge.sock')
    shared_dir_path = find_bridge_path('files')

    async with Connection(socket_path=socket_path, shared_dir_path=shared_dir_path) as connection:
        rclpy.init(args=args)

        bridge = Bridge(connection)

        await asyncio.gather(ros_loop(bridge), bridge.run())

        bridge.destroy_node()
        rclpy.shutdown()

def main(args=None):
    asyncio.run(async_main(args=args))

if __name__ == '__main__':
    main()
