/**
 * Copyright (C) Cloudera, Inc. 2018
 */
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FlafkaConsumer {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put("bootstrap.servers","broker-1.gce.cloudera.com:9092, broker-2.gce.cloudera.com:9092");
        props.put("group.id",args[0]);
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        String topic = args[1];

        consumer.subscribe(Arrays.asList(topic));
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record : records ) {
                System.out.printf("offset = %d, key = %s \n",record.offset(),record.value().split(",")[4]);
               // System.out.printf("offset = %d, key = %s, value = %s \n",record.offset(),record.key(),record.value());

            }
        }
    }
}
