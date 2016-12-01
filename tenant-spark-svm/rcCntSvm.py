#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

import sys
import time
import datetime
import json
import re
import pandas as pd

import presvm

reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext(appName="rcCntSvm")
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

url = "jdbc:mysql://hecom33:3306/hecom_bi"
df = sqlContext.read.format("jdbc").option("url",url).option("driver", "com.mysql.jdbc.Driver").option("user", "bi_user").option("password", "hecom123").option("dbtable","v_record_counting").load()

tdf = df.select(df['id'], df['sample_type'], 
	df['terminals'], df['create_time'], df['update_time'],
	df['province'], df['status'], df['linkcase'], 
	df['type'], df['mylevel'], df['managerlevel'],
	df['into_time'], df['callcount'], df['visitcount'],
	df['traincount'], df['custype'], df['throw_reason'])
tdf = tdf.filter(df['sample_type'] > 0)
tenant = tdf.toPandas()

lenth=len(tenant.index)
start = time.time()
print('load training data spent', time.time()-start)

X_scaled,y,scaler,dummies = presvm.predata_sql(tenant,1)
clf,output = presvm.tenant_svm_train(X_scaled,y)

X,sample_type,cusid = presvm.predata_sql(tenant,-1)   
prob=presvm.tenant_svm(clf,X,scaler,dummies,-1)
probx=prob.tolist()
cusidx=cusid.tolist()
result,ctime=presvm.toscore2(probx)

rclist = list()
ctime = ctime.strftime(u'%Y-%m-%d %H:%M:%S')	
for i in xrange(1,lenth):
	data = '{cusid:%s,predicted:%s,ctime:%s}' % (cusidx[i],result[i],ctime)
	rclist.append(data)

torjson = json.dumps(rclist)

rcpath=re.sub(r'[^0-9]','',str(datetime.date.today()))
sc.parallelize(torjson).saveAsTextFile("/hecom/ods/oms3/rcCnt/"+rcpath)

print(u'*^'*20)
print('end training data spent', time.time()-start)
