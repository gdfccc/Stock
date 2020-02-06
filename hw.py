import tushare as ts
# import site
# ts.set_token('f688c99c1c528c308ef7fc91665f2fa9c18d11911af62659512765b0')
pro = ts.pro_api()
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market,exchange,fullname,enname,list_status,list_date,delist_date,is_hs')
print len(data)
print data[0:3]
# print site.getsitepackages()


import tushare as ts
# ts.set_token('f688c99c1c528c308ef7fc91665f2fa9c18d11911af62659512765b0')
pro = ts.pro_api()
df = pro.daily(ts_code='000001.SZ')
