import asyncio
import logging

from sanic import Sanic
from sanic import Blueprint
from hexi.service import event

_logger = logging.getLogger(__name__)

app = Sanic()


app.static('/', 'hexi/ui/root/index.html')

bp = Blueprint('core', url_prefix='/core')
bp.static('/static', 'hexi/.ui_built')
app.blueprint(bp)


async def on_start(e):
  #app.run(host='0.0.0.0', port=8000, access_log=False)
  #server = app.create_server(host='0.0.0.0', port=8000)
  #asyncio.ensure_future(server)
  server = app.create_server(host="0.0.0.0", port=8000, return_asyncio_server=True)
  loop = asyncio.get_event_loop()
  task = asyncio.ensure_future(server)
  #loop.run_forever()


def init():
  event.subscribe(on_start, ['hexi.start'])
