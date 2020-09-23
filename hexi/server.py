import logging
import logging.config
import sys
import asyncio
import signal
import sanic.config

from hexi.util import taillog

try:
    import uvloop  # type: ignore

    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


_logger = logging.getLogger(__name__)

loop = asyncio.get_event_loop()
signal.signal(signal.SIGINT, lambda s, f: loop.stop())


def load_core_module(BaseClass):
  module = BaseClass()
  module.init()
  module.register()


def main():
  sanic.config.LOGGING['handlers']['memoryTailLog'] = {
    '()': taillog.TailLogHandler,
    'log_queue': taillog.log_queue,
    'filters': ['accessFilter'],
    'formatter': 'simple',
  }
  sanic.config.LOGGING['root'] = {
    'level': 'INFO',
    'handlers': ['internal', 'errorStream', 'memoryTailLog'],
  }
  sanic.config.LOGGING['loggers']['sanic']['level'] = 'INFO'
  sanic.config.LOGGING['loggers']['sanic']['propagate'] = False
  sanic.config.LOGGING['loggers']['sanic']['handlers'].append('memoryTailLog')
  sanic.config.LOGGING['loggers']['network']['propagate'] = False
  sanic.config.LOGGING['loggers']['network']['handlers'].append('memoryTailLog')
  sanic.config.LOGGING['disable_existing_loggers'] = False
  logging.config.dictConfig(sanic.config.LOGGING)

  _logger.info('Loading base modules...')
  from hexi.service import event
  from hexi.service import db
  from hexi.service import plugin
  from hexi.service import web
  from hexi.service import log
  loop.run_until_complete(db.init())
  plugin.init()
  web.init()
  log.init()

  _logger.info('Loading base plugins...')
  from hexi.service.pipeline import InputManager
  from hexi.service.pipeline import MCAManager
  from hexi.service.pipeline import OutputManager
  load_core_module(InputManager.InputManager)
  load_core_module(MCAManager.MCAManager)
  load_core_module(OutputManager.OutputManager)

  _logger.info('Loading external modules...')
  plugin.load()

  _logger.info('Starting...')
  loop.run_until_complete(event.publish('hexi.start', None))
  loop.run_forever()
  loop.run_until_complete(event.publish('hexi.stop', None))

if __name__ == '__main__':
  main()
