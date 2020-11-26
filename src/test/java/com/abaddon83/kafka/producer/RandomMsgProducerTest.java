package com.abaddon83.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class RandomMsgProducerTest {

    @Test
    public void testRecordMsgCreation() {
        ProducerRecord<String, String> record = RandomMsgProducer.createPaymentEvent("1", "settled");
        System.out.println(record.value());
    }
}