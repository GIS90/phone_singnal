# -*- coding: utf-8 -*-

"""
------------------------------------------------
describe: 
------------------------------------------------
"""

from core.define import *
from core.generator import *

__version__ = "v.10"
__author__ = "PyGo"
__time__ = "2016/12/7"

generator = Generaor()
data_type = get_data_type_config()
for dt in data_type:
    if dt == DataType.population:
        generator.population()
