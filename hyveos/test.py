import asyncio
import rclpy
from rclpy.node import Node

from hyveos_msgs.msg import Test as TestMsg
from hyveos_msgs.srv import Test as TestSrv

class Test(Node):
    def __init__(self):
        super().__init__('test')
        self.publisher_ = self.create_publisher(TestMsg, 'test', 10)
        self.service_ = self.create_service(TestSrv, 'test', self._service_callback)

    async def _service_callback(self, request, response):
        response.resp = f'Hello, {request.req}!'
        return response

    async def run(self):
        while rclpy.ok():
            msg = TestMsg()
            msg.data = 'Hello, World!'
            self.publisher_.publish(msg)
            await asyncio.sleep(1)

async def ros_loop(node):
    while rclpy.ok():
        rclpy.spin_once(node, timeout_sec=0)
        await asyncio.sleep(1e-4)

def main(args=None):
    rclpy.init(args=args)

    test = Test()

    future = asyncio.gather(ros_loop(test), test.run())
    asyncio.get_event_loop().run_until_complete(future)

    test.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()
