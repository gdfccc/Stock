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
sql = 'create table stock_basic(\
    ts_code varchar(16) primary key,\
    symbol varchar(16),\
    name varchar(16),\
    area varchar(16),\
    industry varchar(16),\
    fullname varchar(64),\
    market varchar(16),\
    list_status varchar(8),\
    list_date varchar(16),\
    delist_data varchar(16),\
    is_hs varchar(4)\
)CHARSET=utf8'
print(sql)

# 3.执行sql语句
# cursor.execute(sql)

result=cursor.execute(sql) #执行sql语句，返回sql查询成功的记录数目
print(result)

# 关闭连接，游标和连接都要关闭
cursor.close()
conn.close()

# if result:
#     print('登陆成功')
# else:
#     print('登录失败')
