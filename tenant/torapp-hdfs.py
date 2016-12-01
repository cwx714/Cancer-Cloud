#!/usr/bin/python
# -*- coding:utf-8 -*-
from tornado.web import RequestHandler,Application
import json
import re
import datetime

from hdfs import *

class MainHandler(RequestHandler):
    def get(self,*args,**kwargs):
        try:
            client = Client("http://101.201.239.240:50070")
            rcpath = re.sub(r'[^0-9]','',str(datetime.date.today()))
            for i in xrange(0,7):
                fp = "/hecom/ods/oms3/rcCnt/" + rcpath + "/part-0000" + str(i)
                with client.read(fp) as fs:
                    values = fs.read()
                    self.write(values)
        except Exception,e:
            print(e)
            self.write(u'-1')
            return

        if not values:
            self.write(u'-1')
            return

application = Application(
    [(r"/",MainHandler),]
)

if __name__=="__main__":
    from tornado.ioloop import IOLoop

    application.listen(8888)
    IOLoop.instance().start()
