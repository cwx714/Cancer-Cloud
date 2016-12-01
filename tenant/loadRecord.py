#!/usr/bin/python

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext(appName="loadRecord")
sqlContext = SQLContext(sc)

url = "jdbc:mysql://hecom33:3306/hecom_bi"
df = sqlContext.read.format("jdbc").option("url",url).option("driver", "com.mysql.jdbc.Driver").option("user", "root").option("password", "hecom123").option("dbtable","v_record_counting").load()

#print(df.dtypes)
#print(df.columns)
#['id', 'sample_type', 'terminals', 'create_time', 'update_time', 'province', 'status', 'linkcase', 'type', 'mylevel', 'managerlevel', 'into_time', 'callcount', 'visitcount', 'traincount', 'custype', 'throw_reason', 'renew', 'follow_type', 'renew_status', 'first_sign_date', 'cr_lm_count', 'vr_lm_count']
#print(df.head())

#print(df.first())
#print(df.flatMap(lambda p: p.mylevel, p.managerlevel, p.throw_reason, p.custype, p.province, p.type).collect())
#print(df.map(lambda p: p.type).collect())
#print(df.collect())

#print(df.count())
#print(df.distinct().count())

#parts = df.map(lambda l: l.split(","))
#print(parts)
#rcd = parts.map(lambda p: Row(mylevel=p[0], managerlevel=p[1]))
#print(rcd)

df.select(df['mylevel'], df['managerlevel'], df['throw_reason'], df['custype'], df['province'], df['type']).show()
df.groupBy("type").count().show()

#df.select(df['mylevel'], df['managerlevel'], df['throw_reason'], df['custype'], df['province'], df['type']).write.save("/hecom/ods/oms3/record-cnt.parquet", format="parquet")
#rcdParquet = sqlContext.read.parquet("/hecom/ods/oms3/record-cnt.parquet")
#rcdParquet.registerTempTable("record-cnt")
