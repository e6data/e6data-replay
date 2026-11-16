"""The engines the replay can run on. To add one, write a subclass of base.Engine in this
folder and list it here (see docs/adding-an-engine.md)."""
from .databricks import Databricks
from .e6data import E6Data

ENGINES = {cls.name: cls for cls in (E6Data, Databricks)}
