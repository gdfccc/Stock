# -*- coding:utf-8 -*-

import pymysql

# user = input('请输入用户名：')
# pwd = input('请输入密码：')

# 1.连接
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='stock', charset='utf8')
print(conn)

# 2.创建游标
cursor = conn.cursor()

#注意%s需要加引号
# sql = "select * from t1.userinfo where username='%s' and pwd='%s'" %(user, pwd)
def getSql(num):    
    table = "stock_history_{}".format(num)
    sql ='create table {}(\
        ts_code varchar(16),\
        trade_date varchar(16),\
        open float,\
        high float,\
        low float,\
        close float,\
        pre_close float,\
        price_change float,\
        pct_cng float,\
        vol float,\
        amount float,\
        primary key(ts_code,trade_date)\
    )charset=utf8'.format(table)
    return sql
# print(sql)

# 3.执行sql语句
# cursor.execute(sql)

for i in range(20):
    num = i+1
    sql = getSql(str(num))
    print sql
    result=cursor.execute(sql) #执行sql语句，返回sql查询成功的记录数目
    print(result)

# 关闭连接，游标和连接都要关闭
cursor.close()
conn.close()

# if result:
#     print('登陆成功')
# else:
#     print('登录失败')
