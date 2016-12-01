#cancer-kafka-spark
1) data collection
Responsible for real-time data collection from each node, using flume to achieve
2) data access
Because the speed of data acquisition and data processing is not necessarily synchronized, so add a message oriented middleware to be used as a buffer, using Kafka
3) flow calculation
The collected data were analyzed in real time, using spark streaming
4) data output
After the analysis of the results of the persistence to HBase

Server startup command is as follows：
$HADOOP_HOME/sbin/start-dfs.sh
start-hbase.sh
$SPARK_HOME/sbin/start-all.sh
zkServer.sh start
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
$FLUME_HOME/bin/flume-ng agent -c $FLUME_HOME/conf/ -f $FLUME_HOME/conf/flume-conf.properties -Dflume.root.logger=INFO,console -n agent >/dev/null 2>&1 &

spark-submit --class com.cancer.seabox.KafkaToHBase --master spark://cancer-master:7077 /usr/local/app/seabox-kafka-spark-jar-with-dependencies.jar
./flm-out.sh
