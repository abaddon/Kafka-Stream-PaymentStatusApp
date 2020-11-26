package com.abaddon83.kafka.stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LastPaymentStatusTest {

    Gson gson = new GsonBuilder().serializeNulls().create();

    @Test
    public void lastPaymentStatusToJson(){
        LastPaymentStatus paymentStatus = new LastPaymentStatus();


        String json = gson.toJson(paymentStatus);

        System.out.println(json);
    }
    
    @Test
    public void jsonStringToLastPaymentStatus(){
        String json = "{\"status\":\"authorised\",\"amountAuth\":0.0,\"authorisedAt\":null,\"amountSettled\":0.0,\"settledAt\":null,\"amountRefund\":0.0,\"refundedAt\":null}";
        LastPaymentStatus lastPaymentStatus=gson.fromJson(json, LastPaymentStatus.class);

        assertEquals(lastPaymentStatus.status , PaymentStatus.authorised);
    }
}