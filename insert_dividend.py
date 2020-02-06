# -*- coding:utf-8 -*-

import pymysql
import tushare as ts
import math
import numpy
import time
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

def insertDividend(tscode,conn):
    tdata=0
    pro = ts.pro_api()
    num=0
    while True:
        num +=1
        try:
            df = pro.dividend(ts_code=tscode,fields='ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share')
            tdata = df.astype(object).where(pd.notnull(df), None) 
            
        except Exception as e:
            # print e
            print e.message
            print(type(e))
            print dir(e)
            print u"第{}次从ts查询{}数据失败！".format(str(num),tscode)
            if(num<10):
                time.sleep(3)
                continue
            else:
                exit(0)
        else:
            break
    if(tdata.empty):
        print u"{}没有分红数据！".format(tscode)
        return

    sql = 'insert into dividend values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

    vlist = []
    tslist=[]
    for i in tdata.index:
        arr = tdata.iloc[i].map(ecode).tolist()
        tup = tuple(arr)
        # print tup[0]
        vlist.append(tup)

    cursor = conn.cursor()
    for i in range(int(math.ceil(len(vlist)*1.0/PART))):
        # print i
        start = i*PART
        end = len(vlist) if (start+PART)>len(vlist) else (start+PART)
        try:
            result=cursor.executemany(sql,vlist[start:end]) #执行sql语句，返回sql查询成功的记录数目
            print(result)
        except Exception as e:
            print type(e)
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
    querySql = 'select ts_code from stock_basic'
    cursor.execute(querySql)
    result = cursor.fetchall()
    print result
    for t in result:
        print t[0]
        # if(result.index(t)==0):
        #     continue
        insertDividend(t[0],conn)
        time.sleep(1)
    conn.close()


