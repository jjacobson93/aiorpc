import asyncio
import aioamqp
import msgpack
import inspect

class Server(object):
    def __init__(self, queue='', prefetch_count=1, prefetch_size=0, connection_global):
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

    async def raise_exception(self, exception):
        self.response(str(exception), None)

    async def response(self, exception, result):
        payload = msgpack.packb((exception, result))
        await channel.basic_publish(
            payload=payload,
            exchange_name='',
            routing_key=properties.reply_to,
            properties={
                'correlation_id': corr_id
            }
        )

    async def on_request(self, channel, body, envelope, properties):
        corr_id = properties.correlation_id
        try:
            func_name, args, kwargs = msgpack.unpackb(body, use_list=False)
        except ValueError as err:
            # Not enough values to unpack, most likely
            await self.raise_exception(err)
            return
        except err:
            await self.raise_exception(err)
            return

        func = self.functions.get(func_name)
        if func is None:
            await self.raise_exception(f'Unknown function {func_name}')
            return

        try:
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
        except err:
            await self.raise_exception(err)

        await self.response(None, result)

    async def connect(self):
        channel = await protocol.channel()
        await channel.queue_declare(queue_name=self.queue)
        await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
        await channel.basic_consume(self.on_request, queue_name=self.queue)

    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect())
        try:
            loop.run_forever()
        finally:
            loop.close()