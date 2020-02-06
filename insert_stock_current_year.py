# -*- coding:utf-8 -*-

import pymysql
import tushare as ts
import math
import time

def insertOneDay(conn,date,tb_name):
    #get ts data
    time.sleep(1)
    pro = ts.pro_api()
    df = 0
    num=0
    while True:
        num +=1
        try:
            df = pro.daily(trade_date=date)
        except Exception as e:
            print e
            print e.message
            print(type(e))
            print dir(e)
            print u"第{}次从ts查询{}数据失败！".format(str(num),date)
            if(num<10):
                time.sleep(3)
                continue
            else:
                exit(0)
        else:
            break
            # try:
            #     df = pro.daily(ts_code=ts_code)
            # except Exception as e:
            #     print e
            #     print e.message
            #     print u"从ts查询{}日线数据再次失败！".format(ts_code)
            #     exit(0)

    if(df.empty):
        print u"{}没有数据！".format(date)
        return
    
    vlist=[]
    for i in df.index:
        arr = df.iloc[i].tolist()
        for k in range(len(arr)):
            if(k<2):
                # arr[k] = arr[k].encode('utf-8')
                pass
            else:
                arr[k] = float(arr[k])
        tup = tuple(arr)
        vlist.append(tup)

    # 存到数据库
    cursor = conn.cursor()
    sql = 'insert into {} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(tb_name)
    # print sql
    try:
        cursor.executemany(sql,vlist[:])
        conn.commit()
    except Exception as e:
        print e
        # print e.read()
        print u"执行插入{}数据失败，回滚！".format(date)
        conn.rollback()
    cursor.close()

def insertData(conn):
    tbName = "stock_current_year"
    now = time.time()
    lt = time.localtime()
    array = range(lt.tm_yday)
    array.reverse()
    for i in array:
        date = time.strftime("%Y%m%d",time.localtime(now-i*24*3600))
        insertOneDay(conn,date,tbName)

if __name__ == '__main__':

    ts.set_token('f688c99c1c528c308ef7fc91665f2fa9c18d11911af62659512765b0')
    # 1.连接
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='stock', charset='utf8')
    print(conn)

    # 2.创建游标
    cursor = conn.cursor()
    insertData(conn)
    cursor.close()
    # 关闭连接
   
    conn.close()


