from yapsy.IPlugin import IPlugin

class BasePlugin(IPlugin):
  def __init__(self):
    super().__init__()
    self.bp = None;
    self.configurable = False

  def load(self):
    pass