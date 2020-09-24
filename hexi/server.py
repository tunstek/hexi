import logging
import logging.config
import sys
import asyncio
import signal
from sanic import Sanic
from sanic.log import logger
import sanic.config

sys.path.append('C:\\Users\\Keith\\Source\\Repos\\tunstek\\hexi')

from hexi.util import taillog

try:
    import uvloop  # type: ignore

    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


loop = asyncio.get_event_loop()
signal.signal(signal.SIGINT, lambda s, f: loop.stop())


def load_core_module(BaseClass):
  module = BaseClass()
  module.init()
  module.register()


def main():

  app = Sanic('logging_example')

  logger.info('Loading base modules...')
  from hexi.service import event
  from hexi.service import db
  from hexi.service import plugin
  from hexi.service import web
  from hexi.service import log
  loop.run_until_complete(db.init())
  plugin.init()
  web.init()
  log.init()

  logger.info('Loading base plugins...')
  from hexi.service.pipeline import InputManager
  from hexi.service.pipeline import MCAManager
  from hexi.service.pipeline import OutputManager
  load_core_module(InputManager.InputManager)
  load_core_module(MCAManager.MCAManager)
  load_core_module(OutputManager.OutputManager)

  logger.info('Loading external modules...')
  plugin.load()

  logger.info('Starting...')
  loop.run_until_complete(event.publish('hexi.start', None))
  loop.run_forever()
  loop.run_until_complete(event.publish('hexi.stop', None))


if __name__ == '__main__':
  main()
