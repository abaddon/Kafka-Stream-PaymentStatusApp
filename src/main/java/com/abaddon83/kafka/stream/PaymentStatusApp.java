package com.abaddon83.kafka.stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Date;
import java.util.List;
import java.util.Properties;

public class PaymentStatusApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-status-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        PaymentStatusApp paymentStatusApp = new PaymentStatusApp();
        Topology topology = paymentStatusApp.createTopology();

        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.start();

        System.out.println("TOPOLOGY START");
        System.out.println(topology.describe());
        System.out.println("TOPOLOGY END");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public Topology createTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> payment = builder.stream("payment");

        Gson gson = new GsonBuilder().serializeNulls().create();

        payment
                //.filter((key, value) -> List.of("authorised", "settled", "refund", "refund_canceled").contains(value))
                .groupByKey()
                .aggregate(
                        () -> gson.toJson(new LastPaymentStatus()),       //stato iniziale fittizzio
                        (key, newPaymentEvent, currentPaymentStatusJson) -> {
                            // json to currentPaymentStatus
                            LastPaymentStatus currentPaymentStatus = gson.fromJson(currentPaymentStatusJson, LastPaymentStatus.class);
                            // json to paymentEvent received
                            PaymentEvent paymentEvent = gson.fromJson(newPaymentEvent, PaymentEvent.class);
                            // apply event
                            LastPaymentStatus updatedPaymentStatus = currentPaymentStatus.applyEvent(paymentEvent);
                            // updatedPaymentStatus to json
                            return gson.toJson(updatedPaymentStatus);
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                //.filter((key, value) -> value != "new")
                .toStream().to("payment_status");

        return builder.build();
    }
}

class LastPaymentStatus {
    PaymentStatus status;
    Double amountAuth;
    Date authorisedAt;
    Double amountSettled;
    Date settledAt;
    Double amountRefund;
    Date refundedAt;

    LastPaymentStatus(PaymentStatus status, Double amountAuth, Date authorisedAt, Double amountSettled, Date SettledAt, Double amountRefund, Date refundedAt) {
        this.status = status;
        this.amountAuth = amountAuth;
        this.authorisedAt = authorisedAt;
        this.amountSettled = amountSettled;
        this.settledAt = SettledAt;
        this.amountRefund = amountRefund;
        this.refundedAt = refundedAt;
    }

    LastPaymentStatus() {
        status = PaymentStatus.authorised;
        this.amountAuth = 0d;
        this.authorisedAt = null;
        this.amountSettled = 0d;
        this.settledAt = null;
        this.amountRefund = 0d;
        this.refundedAt = null;
    }

    LastPaymentStatus applyEvent(PaymentEvent event) {
        switch (event.status) {
            case authorised:
                applyAuthorisedEvent(event); break;
            case settled:
                applySettledEvent(event);  break;
            case refund:
                applyRefundEvent(event);  break;
        }
        return this;
    }

    private void applyAuthorisedEvent(PaymentEvent event) {
        List acceptableStatus = List.of(PaymentStatus.authorised);
        if (acceptableStatus.contains(status)) {
            this.status = event.status;
        }
        this.amountAuth = event.amount;
        this.authorisedAt = event.executionDt;
    }

    private void applySettledEvent(PaymentEvent event) {
        List acceptableStatus = List.of(PaymentStatus.authorised, PaymentStatus.settled);
        if (acceptableStatus.contains(status)) {
            this.status = event.status;
        }
        this.amountSettled = event.amount;
        this.settledAt = event.executionDt;
    }

    private void applyRefundEvent(PaymentEvent event) {
        List acceptableStatus = List.of(PaymentStatus.authorised, PaymentStatus.settled, PaymentStatus.refund);
        if (acceptableStatus.contains(status)) {
            this.status = event.status;
        }
        this.amountRefund = event.amount;
        this.refundedAt = event.executionDt;
    }

}


class PaymentEvent {
    PaymentStatus status;
    Double amount;
    Date executionDt;

    PaymentEvent(PaymentStatus status, Double amount, Date executionDt) {
        this.status = status;
        this.amount = amount;
        this.executionDt = executionDt;
    }
}

enum PaymentStatus {
    authorised,
    settled,
    refund
}
