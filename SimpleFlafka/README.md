# Simple Flafka

This contains the sample code for the "Scalability of Kafka Messaging using Consumer Groups"

## Instructions

First, run the Flume agent:

<pre>
$ sudo -u hdfs flume-ng agent -n flume/TwitterAgent -f etc/twitter.conf --conf etc/flume-ng/conf/
</pre>

Then, you can use the kafka-console-consumer command to begin consuming the topic.

<pre>
$ cp /etc/conf/tools-log4j.properties consumer.properties
$ kafka-console-consumer --zookeeper <zkhost>:2181 --topic "tweets" --consumer.config consumer.properties
</pre>
