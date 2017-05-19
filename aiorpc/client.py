import aioamqp
import asyncio
import msgpack
from uuid import uuid4

class Client(object):
    def __init__(self, queue='', timeout=None):
        self._transport = None
        self._protocol = None
        self._channel = None
        self._callback_queue = None
        self._queue = queue
        self._waiter = asyncio.Event()

    async def _connect(self, *args, **kwargs):
        """ an `__init__` method can't be a coroutine"""
        self._transport, self._protocol = await aioamqp.connect(*args, **kwargs)
        self._channel = await self._protocol.channel()

        result = await self._channel.queue_declare(queue_name='', exclusive=True)
        self._callback_queue = result['queue']

        await self._channel.basic_consume(
            self._on_response,
            no_ack=True,
            queue_name=self._callback_queue,
        )

    async def _on_response(self, channel, body, envelope, properties):
        if self._corr_id == properties.correlation_id:
            self._response = body

        self._waiter.set()

    async def __call__(self, method, *args, **kwargs):
        if not self._protocol:
            await self._connect()
        self._response = None
        self._corr_id = str(uuid4())

        payload = msgpack.packb((method, args, kwargs))
        await self._channel.basic_publish(
            payload=payload,
            exchange_name='',
            routing_key=self._queue,
            properties={
                'reply_to': self._callback_queue,
                'correlation_id': self._corr_id,
            },
        )
        await self._waiter.wait()
        await self._protocol.close()
        return self._response

    def __getattr__(self, method):
        @wraps(self.__call__)
        async def wrapper(*args, **kwargs):
            return await self(method, *args, **kwargs)
        return wrapper
