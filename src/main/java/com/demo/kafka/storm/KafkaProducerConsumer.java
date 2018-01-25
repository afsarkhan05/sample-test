package com.demo.kafka.storm;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 * Created by afsar.khan on 12/23/17.
 */
public class KafkaProducerConsumer {

    private static final Logger LOG = Logger.getLogger(KafkaProducerConsumer.class);
    //private static final String topicName = "feed.event.lld-processor.int";//"feed.event.lld-transformer.int"
    //public static final String message = "{\"hdfsHost\":\"tbhortonctl1.int.iad2.xaxis.net\",\"hdfsPort\":\"8020\",\"providerId\":\"4\",\"dataSourceId\":\"855\",\"filePath\":\"/user/Zaman-Data/ttd/impressions_2m0611k_V5_1_2017-02-11T081840_2017-02-11T082407_2017-02-11T083023_dsadsadsadasdsadasd.log\"}";


    private static final String producerTopicName = "feed.event.lld-processor.int";//"feed.event.lld-transformer.int"
    private static final String consumerTopicName = "feed.event.lld-transformer.int";
    public static final String message = "{\"hdfsHost\":\"tbhortonctl1.int.iad2.xaxis.net\",\"hdfsPort\":\"8020\",\"providerId\":\"4\",\"dataSourceId\":\"855\",\"filePath\":\"/user/Zaman-Data/ttd/impressions_2m0611k_V5_1_2017-02-11T081840_2017-02-11T082407_2017-02-11T083023_dsadsadsadasdsadasd.log\"}";
    //public static final String message = "{\"hdfsHost\":\"tbhortonctl1.int.iad2.xaxis.net\",\"hdfsPort\":\"8020\",\"providerId\":\"4\",\"dataSourceId\":\"869\",\"filePath\":\"/user/Zaman-Data/dfa/dcm_account6538_impression_2017091215_20170912_211732_609023593.csv\"}";

    private static final String messageToCheck="Zaman-Data";
    private static final int totalMessagesSent = 10;

    private static final String BOOT_STRAP_SERVERS = "tbkafka1.int.iad2.xaxis.net:9092";
    private static final String ZOO_KEEPER_CONNECT = "tbkafka1.int.iad2.xaxis.net:2181";

    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private static final String ACKS = "1";
    private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void main(String[] args) throws Exception {

        produce(producerTopicName, message);

       if(! consumer(consumerTopicName, messageToCheck, totalMessagesSent)){
           System.out.println("Either all messages not consumed or not found for message text: "+ messageToCheck);
       }else{
           System.out.println("All messages consumed or found for message text: "+ messageToCheck);
       }


    }

    private static void produce(String topicName, String message){
        Properties props = getProducerConfig();
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        try {
            ProducerRecord<Long, String> record =  new ProducerRecord(topicName, "10000", message);
            producer.send(record);
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }

    private static Properties getProducerConfig(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOT_STRAP_SERVERS);
        props.put("zk.connect", ZOO_KEEPER_CONNECT);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", ACKS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static Properties getConsumerConfig(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOT_STRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }


    private static Consumer<Long, String> createConsumer(String topicName) {
        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer(getConsumerConfig());
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }

    private static boolean consumer(String topicName, String checkString, int totalImpSent) {
        boolean found = false;
        int loop = 10000;
        final Consumer<Long, String> consumer = createConsumer(topicName);
        try {
            int count = 0;
            for (int i = 0; i < 60; i++) {
                Thread.sleep(1000l);
                ConsumerRecords<Long, String> records = consumer.poll(100);
                for (ConsumerRecord<Long, String> record : records) {
                    if (record.value().contains(checkString)) {
                        //System.out.println("offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
                        System.out.println("Message Text: " + checkString + " found :" + ++count);

                        if (totalImpSent == count) {
                            return true;
                        }
                    }
                }

            }
        } catch (Exception e) {

        } finally {
            consumer.close();
            System.out.println("DONE");
        }
        return found;
    }
}
