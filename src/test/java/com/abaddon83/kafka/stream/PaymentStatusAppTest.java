package com.abaddon83.kafka.stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class PaymentStatusAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();

    private Gson gson = new GsonBuilder().serializeNulls().create();
    private SimpleDateFormat dateFormatter=new SimpleDateFormat("MMM dd, yyyy, hh:mm:ss a");

    @Before
    public void setup() {
        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "payment-status-app");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        PaymentStatusApp paymentStatusApp = new PaymentStatusApp();
        Topology topology = paymentStatusApp.createTopology();

        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("payment", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("payment_status", stringSerde.deserializer(), stringSerde.deserializer());

    }

    @Test
    public void testOneEventStream() {
        inputTopic.pipeInput("1", "{\"status\":\"authorised\",\"amount\":10.0,\"executionDt\":\"Nov 25, 2020, 9:05:10 PM\"}");
        KeyValue<String, String> message = outputTopic.readKeyValue();
        LastPaymentStatus lastPaymentStatus = gson.fromJson(message.value, LastPaymentStatus.class);
        assertEquals(PaymentStatus.authorised, lastPaymentStatus.status);
        assertEquals(Double.valueOf("10.0"), lastPaymentStatus.amountAuth);
    }

    @Test
    public void testAllEventStream() throws ParseException {
        inputTopic.pipeInput("1", "{\"status\":\"authorised\",\"amount\":10.0,\"executionDt\":\"Nov 25, 2020, 9:05:10 PM\"}");
        inputTopic.pipeInput("1", "{\"status\":\"settled\",\"amount\":11.0,\"executionDt\":\"Nov 26, 2020, 9:05:10 PM\"}");
        inputTopic.pipeInput("1", "{\"status\":\"refund\",\"amount\":12.0,\"executionDt\":\"Nov 27, 2020, 9:05:10 PM\"}");

        //KeyValue<String,String> message = outputTopic.readKeyValue();
        List<TestRecord<String, String>> list = outputTopic.readRecordsToList();

        assertEquals(3, list.size());

        list.forEach( msg ->{
            System.out.println(msg.getValue());
        });
        //pick up the last one
        String lastMessageValue = list.get(list.size()-1).getValue();
        LastPaymentStatus lastPaymentStatus = gson.fromJson(lastMessageValue, LastPaymentStatus.class);
        System.out.println(lastPaymentStatus);

        assertEquals(PaymentStatus.refund, lastPaymentStatus.status);
        assertEquals(Double.valueOf("10.0"), lastPaymentStatus.amountAuth);
        assertEquals(Double.valueOf("11.0"), lastPaymentStatus.amountSettled);
        assertEquals(Double.valueOf("12.0"), lastPaymentStatus.amountRefund);
        assertEquals(dateFormatter.parse("Nov 25, 2020, 9:05:10 PM"),lastPaymentStatus.authorisedAt);
        assertEquals(dateFormatter.parse("Nov 26, 2020, 9:05:10 PM"),lastPaymentStatus.settledAt);
        assertEquals(dateFormatter.parse("Nov 27, 2020, 9:05:10 PM"),lastPaymentStatus.refundedAt);


    }

    @Test
    public void testAllEventNotOrderedStream() throws ParseException {
        inputTopic.pipeInput("1", "{\"status\":\"settled\",\"amount\":11.0,\"executionDt\":\"Nov 26, 2020, 9:05:10 PM\"}");
        inputTopic.pipeInput("1", "{\"status\":\"refund\",\"amount\":12.0,\"executionDt\":\"Nov 27, 2020, 9:05:10 PM\"}");
        inputTopic.pipeInput("1", "{\"status\":\"authorised\",\"amount\":10.0,\"executionDt\":\"Nov 25, 2020, 9:05:10 PM\"}");

        //KeyValue<String,String> message = outputTopic.readKeyValue();
        List<TestRecord<String, String>> list = outputTopic.readRecordsToList();

        assertEquals(3, list.size());

        list.forEach( msg ->{
            System.out.println(msg.getValue());
        });
        //pick up the last one
        String lastMessageValue = list.get(list.size()-1).getValue();
        LastPaymentStatus lastPaymentStatus = gson.fromJson(lastMessageValue, LastPaymentStatus.class);
        System.out.println(lastPaymentStatus);

        assertEquals(PaymentStatus.refund, lastPaymentStatus.status);
        assertEquals(Double.valueOf("10.0"), lastPaymentStatus.amountAuth);
        assertEquals(Double.valueOf("11.0"), lastPaymentStatus.amountSettled);
        assertEquals(Double.valueOf("12.0"), lastPaymentStatus.amountRefund);
        assertEquals(dateFormatter.parse("Nov 25, 2020, 9:05:10 PM"),lastPaymentStatus.authorisedAt);
        assertEquals(dateFormatter.parse("Nov 26, 2020, 9:05:10 PM"),lastPaymentStatus.settledAt);
        assertEquals(dateFormatter.parse("Nov 27, 2020, 9:05:10 PM"),lastPaymentStatus.refundedAt);


    }

    @After
    public void tearDown() {
        testDriver.close();
    }

}