from abc import ABC, abstractmethod
import asyncio
from pathlib import Path
from signal import SIGINT, SIGTERM
import traceback

from hyveos_sdk import Connection, OpenedConnection
import rclpy
from rclpy.node import Node


def service_callback(f):
    async def inner_wrapper(self, request, response):
        try:
            return await f(self, request, response)
        except Exception as e:
            response.success = False
            response.error = str(e)
            return response

    # Allows calling __await__ repeatedly on awaitables that require waiting for a future before
    # doing so (e.g. asyncio). This will make asyncio functions compatible with rclpy
    # implementation of async. See https://github.com/ros2/rclpy/issues/962 for more info.
    async def wrapper(self, request, response):
        coro = inner_wrapper(self, request, response)
        try:
            while True:
                future = coro.send(None)
                assert asyncio.isfuture(future) or future is None, \
                    'Unexpected awaitable behavior. Only use rclpy or asyncio awaitables.'
                if future is None:
                    # coro is rclpy-style awaitable; await is expected to be called repeatedly.
                    await asyncio.sleep(0)
                    continue
                while not future.done():
                    # coro is asyncio-style awaitable; stop calling await until future is done.
                    await asyncio.sleep(0)  # yields None
                future.result()
        except StopIteration as e:
            return e.value

    return wrapper


def prepare_data(data: bytes | list[bytes]) -> bytes:
    if isinstance(data, bytes):
        return data
    elif isinstance(data, list):
        return b''.join(data)
    else:
        raise ValueError('Invalid data')


class BridgeClient(ABC):

    @abstractmethod
    def __init__(self, node: 'Bridge'):
        pass

    @abstractmethod
    async def run(self):
        pass


class Bridge(Node):
    connection: OpenedConnection
    bridge_clients: list[BridgeClient]

    def __init__(self, connection: OpenedConnection):
        super().__init__('hyveos_bridge')

        from .reqres import ReqResClient as _  # noqa: F401

        self.connection = connection
        self.bridge_clients = [client(self) for client in BridgeClient.__subclasses__()]

        for client in self.bridge_clients:
            self.get_logger().info(f'Initializing {client.__class__.__name__}')

    async def run(self):
        coroutines = [client.run() for client in self.bridge_clients]
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
        try:
            rclpy.init(args=args)

            bridge = Bridge(connection)

            await asyncio.gather(ros_loop(bridge), bridge.run())
        except asyncio.CancelledError:
            print('Exiting...')
        except Exception:
            traceback.print_exc()
        finally:
            if rclpy.ok():
                bridge.destroy_node()
                rclpy.shutdown()


def main(args=None):
    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(async_main(args=args))
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, main_task.cancel)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()


if __name__ == '__main__':
    main()
