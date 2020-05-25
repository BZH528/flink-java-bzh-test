package com.bzh.bigdata.producer;


import com.bzh.bigdata.domain.WordCount;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @description:
 * @author: bizhihao
 * @createDate: 2020/5/25
 * @version: 1.0
 */
public class KafkaProducer {
    private static  final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {

         Properties kafkaProps = new Properties();

         kafkaProps.put("bootstrap.servers","192.168.2.111:9092,192.168.2.112:9092,192.168.2.113:9092");
         kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new org.apache.kafka.clients.producer.KafkaProducer<String,String>(kafkaProps);

        LOGGER.info("producer:" + producer);

        ProducerRecord<String, String> record = new ProducerRecord<>("Fruit", "pear");

        try {
            Future send = producer.send(record);
            System.out.println(send.isDone());
            System.out.println(send.get());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源才会发送消息
            producer.close();
        }


    }
}
