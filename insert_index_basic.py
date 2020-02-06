# -*- coding:utf-8 -*-

import pymysql
import tushare as ts
import math
import numpy
import pandas as pd

PART = 1000

def ecode(x):   
    if(x):
        if(type(x) == numpy.float64 or type(x) == float):
            return float(x)
        else:
            try:
                if(len(x)>512):
                    x=x[:508]
                return x.encode('utf-8')
            except Exception as e:
                print e.message
                print x
                print type(x)
                exit(0)
    else:
        return x

def insertIndexBasic(conn,market):
    tdata=0
    pro = ts.pro_api()
    try:
        df = pro.index_basic(market = market,fields='ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date')
        tdata = df.astype(object).where(pd.notnull(df), None) 
        print len(tdata)
        print tdata.head()
        # print tdata.describe
    except Exception as e:
        # print e
        print e.message
        exit(0)
    
    sql = 'insert into index_basic values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

    vlist = []
    tslist=[]
    for i in tdata.index:
        arr = tdata.iloc[i].map(ecode).tolist()
        tup = tuple(arr)
        # print tup[0]
        vlist.append(tup)

    # print len(tslist)
    # s=set(tslist)
    # print len(s)

    cursor = conn.cursor()
    for i in range(int(math.ceil(len(vlist)*1.0/PART))):
        # print i
        start = i*PART
        end = len(vlist) if (start+PART)>len(vlist) else (start+PART)
        try:
            result=cursor.executemany(sql,vlist[start:end]) #执行sql语句，返回sql查询成功的记录数目
            print(result)
        except Exception as e:
            print e
            print e.message
            exit(0)
    
    cursor.close()
    conn.commit()

if __name__ == '__main__':

    ts.set_token('f688c99c1c528c308ef7fc91665f2fa9c18d11911af62659512765b0')
    # 1.连接
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='stock', charset='utf8')
    print(conn)

    cursor = conn.cursor()
    querySql = 'select code from market'
    cursor.execute(querySql)
    result = cursor.fetchall()
    print result
    for t in result:
        print t[0]
        # if(result.index(t)==0):
        #     continue
        insertIndexBasic(conn,t[0])
    conn.close()


