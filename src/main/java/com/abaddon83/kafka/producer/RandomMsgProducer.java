package com.abaddon83.kafka.producer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class RandomMsgProducer {

    public static String PAYMENT_TOPIC = "payment";
    private static SimpleDateFormat dateFormatter=new SimpleDateFormat("MMM dd, yyyy, hh:mm:ss a");

    public static void main(String[] args) {
        Properties properties = new Properties();
        //bootstrap server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3"); //retry tentatives
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        //leverage idempotent producer
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        //init Kafka producer
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);

        generateRandomEvents(100).forEach(record ->{

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("offset: "+recordMetadata.offset());
                    if(e != null){
                        System.out.println("ERROR: "+e.getMessage());
                    }
                }
            });
            //Thread.sleep(100);
        });

    }

    private static List<ProducerRecord<String,String>> generateRandomEvents(int numPayments){
        List<ProducerRecord<String,String>> list = Collections.emptyList();
        int i =0;
        while(i < numPayments){
            i++;
            list.addAll(createPaymentEvents("pId-"+numPayments));
        }
        Collections.shuffle(list);
        return list;
    }

    private static List<ProducerRecord<String,String>> createPaymentEvents(String paymentId){
        List<ProducerRecord<String,String>> list = Collections.emptyList();

        list.add(createPaymentEvent(paymentId,"authorised"));
        list.add(createPaymentEvent(paymentId,"settled"));
        list.add(createPaymentEvent(paymentId,"refund"));

        return list;
    }

    public static ProducerRecord<String,String> createPaymentEvent(String paymentId, String paymentStatus){

        ObjectNode event = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0,100);
        Instant now = Instant.now();
        event.put("status",paymentStatus);
        event.put("amount",amount);
        event.put("executionDt", dateFormatter.format(GregorianCalendar.getInstance().getTime()));
        return new ProducerRecord<>(PAYMENT_TOPIC,paymentId,event.toString());
    }
}
