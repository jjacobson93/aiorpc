import asyncio
import aioamqp
import umsgpack as msgpack
import inspect
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

class Server(object):
    def __init__(self, queue='', prefetch_count=1, prefetch_size=0, connection_global=False):
        self.queue = queue
        self.prefetch_count = prefetch_count
        self.prefetch_size = prefetch_size
        self.connection_global = connection_global
        self.functions = {}

    def __call__(self, func):
        if not callable(func):
            def decorator(f):
                self.functions[func] = f
                return f
            return decorator
        else:
            self.functions[func.__name__] = func
            return func

    async def raise_exception(self, exception, correlation_id):
        self.response(str(exception), None, correlation_id)

    async def response(self, exception, result, correlation_id):
        payload = msgpack.packb((exception, result))
        logger.info(f'Sending response to queue {properties.reply_to} ({correlation_id})')
        await channel.basic_publish(
            payload=payload,
            exchange_name='',
            routing_key=properties.reply_to,
            properties={
                'correlation_id': correlation_id
            }
        )

    async def on_request(self, channel, body, envelope, properties):
        correlation_id = properties.correlation_id
        try:
            func_name, args, kwargs = msgpack.unpackb(body)
            logger.info(f'Received request for {func_name} ({correlation_id})')
        except err:
            logger.error(f'Could not unpack message: {err} ({correlation_id})')
            await self.raise_exception(err, correlation_id)
            return

        func = self.functions.get(func_name)
        if func is None:
            logger.error(f'Function {func_name} does not exist ({correlation_id})')
            await self.raise_exception(f'Unknown function {func_name}', correlation_id)
            return

        try:
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
        except err:
            logger.error(f'Exception while executing {func_name}: {err} ({correlation_id})')
            await self.raise_exception(err, correlation_id)

        await self.response(None, result, correlation_id)

    async def connect(self, *args, **kwargs):
        retry = kwargs.get('retry', 5) # retry every X second(s)
        if 'retry' in kwargs:
            del kwargs['retry']

        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port')
        ssl = kwargs.get('ssl', False)
        if port is None:
            port = 5671 if ssl else 5672

        protocol = None
        if retry is not False:
            while protocol is None:
                try:
                    transport, protocol = await aioamqp.connect(*args, **kwargs)
                except:
                    logger.warn(f'Could not connect to amqp://{host}:{port}/. Trying again in {retry} second(s).')
                    await asyncio.sleep(retry)
        else:
            transport, protocol = await aioamqp.connect(*args, **kwargs)

        logger.info(f'Connected to amqp://{host}:{port}/.')
        channel = await protocol.channel()
        await channel.queue_declare(queue_name=self.queue)
        await channel.basic_qos(
            prefetch_count=self.prefetch_count,
            prefetch_size=self.prefetch_size,
            connection_global=self.connection_global
        )
        await channel.basic_consume(self.on_request, queue_name=self.queue)
        logger.info(f'Consuming on queue {self.queue}.')

    def start(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect(*args, **kwargs))
        try:
            loop.run_forever()
        finally:
            loop.close()