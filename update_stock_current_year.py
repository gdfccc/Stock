# -*- coding:utf-8 -*-

import pymysql
import tushare as ts
import math
import time

def insertOneDay(conn,date,tb_name):
    #get ts data
    print date
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

def updateData(conn):
    tbName = "stock_current_year"
    lastDay = queryLastDay(conn)
    lastDayStruct = time.strptime(lastDay,'%Y%m%d')

    now = time.time()
    lt = time.localtime()
    if(lt.tm_yday<lastDayStruct.tm_yday):
        lt = time.strptime(str(lastDayStruct.tm_year)+"1231",'%Y%m%d')
    array = range(lt.tm_yday-lastDayStruct.tm_yday)
    array.reverse()
    for i in array:
        date = time.strftime("%Y%m%d",time.localtime(now-i*24*3600))
        insertOneDay(conn,date,tbName)

def queryLastDay(conn):
    cursor = conn.cursor()
    sql='select distinct trade_date from stock_current_year order by trade_date desc limit 1'
    try:
        cursor.execute(sql)
        result = cursor.fetchone()
        # print result
        return result[0]
    except Exception as e:
        print e
        # print e.read()
        print u"查询数据库失败，sql为："+sql
        exit(0)



if __name__ == '__main__':

    ts.set_token('f688c99c1c528c308ef7fc91665f2fa9c18d11911af62659512765b0')
    # 1.连接
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='stock', charset='utf8')
    print(conn)
    # 2.创建游标
    updateData(conn)
    print u"更新到："+queryLastDay(conn)
   
    conn.close()


