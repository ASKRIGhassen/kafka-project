package com.kafka.samples.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.processing.ProcessingEnvironment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerStarterApp {


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "testApp01");

        Consumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

        List<String> topics = new ArrayList<String>();
        topics.add("mysecondtopic");
        myConsumer.subscribe(topics);

        try {
            while (true) {
                Duration duration = Duration.ofMillis(100);
                ConsumerRecords<String, String> records = myConsumer.poll(duration);
                records.forEach(record -> {
                    System.out.println(String.format("Topic : %s , Partition : %s , Timestamp : %d , Value : %s",
                            record.topic(), record.partition(), record.timestamp(), record.value()));
                });
            }

        } finally {
            myConsumer.close();
        }


    }
}
