try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = '0.1.12'

install_requires = open('requirements.txt').read().strip().split()
setup(
    name='aiorpc',
    description='A Python RPC library using RabbitMQ (aioamqp) and asyncio',
    version=version,
    url='https://github.com/jjacobson93/aiorpc',
    author='Jeremy Jacobson',
    author_email='jjacobson93@gmail.com',
    packages=['aiorpc'],
    install_requires=install_requires,
    license='MIT'
)