# -*- coding:utf-8 -*-

import pymysql
import tushare as ts
import math

def ecode(x):
    if(x):
        return x.encode('utf-8')
    else:
        return x

#get ts data
pro = ts.pro_api()
tdata=0
try:
    tdata = pro.stock_basic(exchange='', fields='ts_code,symbol,name,area,industry,fullname,market,list_status,list_date,delist_date,is_hs')
    print len(tdata)
    print tdata.head()
    print tdata.describe
except Exception as e:
    print e
    print e.message  

# user = input('请输入用户名：')
# pwd = input('请输入密码：')

# 1.连接
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='stock', charset='utf8')
print(conn)

# 2.创建游标
cursor = conn.cursor()

#注意%s需要加引号
sql = 'insert into stock_basic values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
# print(sql)

# 3.执行sql语句
# cursor.execute(sql)

PART = 1000

vlist = []


# for i in tdata.index:
#     arr = tdata.iloc[i].map(ecode).tolist()
#     tup = tuple(arr)
#     vlist.append(tup)

# for i in range(int(math.ceil(len(vlist)*1.0/PART))):
#     print i
#     start = i*PART
#     end = len(vlist) if (start+PART)>len(vlist) else (start+PART)
#     result=cursor.executemany(sql,vlist[start:end]) #执行sql语句，返回sql查询成功的记录数目
#     print(result)

# conn.commit()
# 关闭连接，游标和连接都要关闭
cursor.close()
conn.close()

# if result:
#     print('登陆成功')
# else:
#     print('登录失败')
