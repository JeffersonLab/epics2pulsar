package org.jlab;

import org.apache.pulsar.client.api.*;

import java.io.IOException;

public class Subscriber {

    public void start() throws IOException {
        String pulsarUrl = System.getenv("PULSAR_URL");

        if(pulsarUrl == null) {
            throw new IOException("Environment variable PULSAR_URL not found");
        }

        try(PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build()) {

            String pv = "iocin1:heartbeat";

            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(pv)
                    .subscriptionName("subscription1")
                    .subscribe();

            while(true) {
                Message msg = consumer.receive();

                System.out.println(new String(msg.getData()));

                consumer.acknowledge(msg);
            }

        }
    }

    public static void main(String[] args) throws IOException {
        Subscriber sub = new Subscriber();

        sub.start();
    }
}
