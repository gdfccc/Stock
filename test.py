# -*- coding:utf-8 -*-
import tushare as ts
import math
import time
try:
    pro = ts.pro_api()
    df = pro.index_basic(market='SSE')
    print df.empty
except Exception as e:
    print e.message
    print(type(e))
    print dir(e)