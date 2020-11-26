package com.abaddon83.kafka.stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;

import java.util.GregorianCalendar;

import static org.junit.Assert.*;

public class PaymentEventTest {

    Gson gson = new GsonBuilder().serializeNulls().create();

    @Test
    public void paymentEventToJson(){
        PaymentEvent event = new PaymentEvent(PaymentStatus.authorised,10.00, GregorianCalendar.getInstance().getTime());


        String json = gson.toJson(event);

        System.out.println(json);
    }

    @Test
    public void jsonStringToPaymentEvent(){
        String json = "{\"status\":\"authorised\",\"amount\":10.0,\"executionDt\":\"Nov 25, 2020, 9:05:10 PM\"}";
        PaymentEvent paymentEvent=gson.fromJson(json,PaymentEvent.class);

        assertEquals(paymentEvent.status , PaymentStatus.authorised);
    }
}