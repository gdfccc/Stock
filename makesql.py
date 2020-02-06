# -*- coding:utf-8 -*-
f=open('sqlfile','w')
for i in range(20):
    k=i+1
    sql = "update stock_basic set his_table_num={} where ts_code in (select distinct ts_code from stock_history_{});\n".format(k,k)
    f.write(sql)
f.close()