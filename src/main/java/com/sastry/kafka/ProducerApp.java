package com.sastry.kafka;

import org.apache.kafka.clients.producer.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ProducerApp {

    public static void main(String[] args) {

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        String topic = "test";
        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        int numberOfRecords = 100; // number of records to send
        long sleepTimer = 0; // how long you want to wait before the next record to be sent
        try {

            ProducerRecord<String,String> producerRecord =  new ProducerRecord<String, String>(topic,"My Java message");
            myProducer.send(producerRecord);
                } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

    }
}
