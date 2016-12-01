#!/usr/bin/python
# -*- coding:utf-8 -*-
from tornado.web import RequestHandler,Application
import torndb
import json
from datetime import datetime

class MainHandler(RequestHandler):
    def get(self,*args,**kwargs):
        limit = self.get_argument('limit',None)
        start = self.get_argument('start',None)
        dt=self.get_argument('dt',None)

        if not limit:
            limit=50
        if not start:
            start=0
        if not dt:
            dt=datetime.now().strftime(u'%Y%m%d')

        tdb = torndb.Connection(
            "rdsxfv309s0529z3t6ay.mysql.rds.aliyuncs.com",
            "bigdata",user="hecom_bi",password="BImysql2019*"
        )

        csql = 'SELECT * FROM tenant_predict%s limit %s, %s ' % (dt, start, limit)

        try:
            values = tdb.query(csql)
        except Exception,e:
            self.write(u'-1')
            return

        if not values:
            self.write(u'-1')
            return

        result=list()
        for row in values:
            ctime=row[u'ctime'].strftime(u'%Y-%m-%d %H:%M:%S')
            row[u'ctime']=ctime
            result.append(row)

        self.write(json.dumps(result))

application = Application(
    [(r"/",MainHandler),]
)

if __name__=="__main__":
    from tornado.ioloop import IOLoop

    application.listen(8888)
    IOLoop.instance().start()
