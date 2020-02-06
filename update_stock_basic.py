# -*- coding:utf-8 -*-

import pymysql
import tushare as ts
import math
import time

def ecode(x):
    if(x):
        return x.encode('utf-8')
    else:
        return x

if __name__ == '__main__':

    ts.set_token('f688c99c1c528c308ef7fc91665f2fa9c18d11911af62659512765b0')
    # 1.连接
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='stock', charset='utf8')
    print(conn)
    # 2.查询需要更新的ts_code
    cursor = conn.cursor()
    querySql = 'select distinct ts_code from stock_current_year where ts_code not in (select ts_code from stock_basic)'
    tscodelist = []
    try:
        cursor.execute(querySql)
        ts_code_tup = cursor.fetchall()
        for item in ts_code_tup:
            tscodelist.append(item[0])
        print tscodelist
    except Exception as e:
        print e
        print u"查询需要更新的ts_code失败！"
        cursor.close()
        conn.close()
        exit(0)
    cursor.close()
    
    #get ts data
    pro = ts.pro_api()
    tdata=0
    num=0
    while True:
        num +=1
        try:
            tdata = pro.stock_basic(exchange='', fields='ts_code,symbol,name,area,industry,fullname,market,list_status,list_date,delist_date,is_hs')
        except Exception as e:
            print e
            print e.message
            print(type(e))
            print dir(e)
            print u"第{}次从ts查询stock_basic数据失败！".format(str(num))
            if(num<10):
                time.sleep(3)
                continue
            else:
                print u"程序退出！"
                exit(0)
        else:
            break
    
    vlist = []   
    for i in tdata.index:
        if(tdata.iloc[i][0] not in tscodelist):
            continue
        arr = tdata.iloc[i].map(ecode).tolist()
        tup = tuple(arr)
        vlist.append(tup)
    if(vlist):
        insertSql =  'insert into stock_basic(ts_code,symbol,name,area,industry,fullname,market,list_status,list_date,delist_data,is_hs) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cur = conn.cursor()
        result=cur.executemany(insertSql,vlist)
        print result
        conn.commit()
        print u"stock_basic更新完成！"
        cur.close()
    else:
        print u"没有需要更新的stock_basic信息！"
    conn.close()


