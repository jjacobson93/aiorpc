import aioamqp
import asyncio
import msgpack
import logging
from functools import wraps
from uuid import uuid4

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

class Client(object):
    def __init__(self, queue='', host='localhost', port=None, ssl=False):
        self._transport = None
        self._protocol = None
        self._channel = None
        self._callback_queue = None
        self._queue = queue
        self._host = host
        self._port = port
        self._ssl = ssl
        self._waiter = asyncio.Event()

    async def _connect(self, *args, **kwargs):
        """ an `__init__` method can't be a coroutine"""
        self._transport, self._protocol = await aioamqp.connect(*args, **kwargs)
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port')
        ssl = kwargs.get('ssl', False)
        if port is None:
            port = 5671 if ssl else 5672

        logger.info(f'Connected to amqp://{host}:port/')
        self._channel = await self._protocol.channel()

        result = await self._channel.queue_declare(queue_name='', exclusive=True)
        self._callback_queue = result['queue']
        logger.info(f'Created callback queue: {self._callback_queue}')

        await self._channel.basic_consume(
            self._on_response,
            no_ack=True,
            queue_name=self._callback_queue,
        )

    async def _on_response(self, channel, body, envelope, properties):
        if self._corr_id == properties.correlation_id:
            self._response = body
            logger.info(f'Received response for {self._corr_id}')

        self._waiter.set()

    async def __call__(self, method, *args, **kwargs):
        if not self._protocol:
            await self._connect(host=self._host, port=self._port, ssl=self._ssl)
        self._response = None
        self._corr_id = str(uuid4())

        payload = msgpack.packb((method, args, kwargs))
        logger.info(f'Publishing to {self._queue}: {method} ({self._corr_id})')
        await self._channel.basic_publish(
            payload=payload,
            exchange_name='',
            routing_key=self._queue,
            properties={
                'reply_to': self._callback_queue,
                'correlation_id': self._corr_id,
            },
        )

        logger.info(f'Waiting for response on queue {self._callback_queue} ({self._corr_id})')
        await self._waiter.wait()
        await self._protocol.close()
        return self._response

    def __getattr__(self, method):
        @wraps(self.__call__)
        async def wrapper(*args, **kwargs):
            return await self(method, *args, **kwargs)
        return wrapper