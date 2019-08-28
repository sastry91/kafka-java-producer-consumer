package com.sastry.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;


import java.util.*;

public class ConsumerApp {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "console-consumer-47047");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);
        myConsumer.subscribe(Arrays.asList("test"));

        try {
            while (true) {
                ConsumerRecords records = myConsumer.poll(100);

                printRecords(records);
            }
        } catch (Exception ex) {
            System.out.println("Exception in consuming records " + ex);
        } finally {
            myConsumer.close();
        }

    }

    private static void printSet(Set<TopicPartition> collection) {
        if (collection.isEmpty()) {
            System.out.println("I do not have any partitions assigned yet...");
        } else {
            System.out.println("I am assigned to following partitions:");
            for (TopicPartition partition : collection) {
                System.out.println(String.format("Partition: %s in Topic: %s", Integer.toString(partition.partition()), partition.topic()));
            }
        }
    }

    private static void printRecords(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }
}
