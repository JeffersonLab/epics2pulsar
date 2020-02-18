package org.jlab;

import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Subscriber {

    public void start() throws IOException {
        String pulsarUrl = System.getenv("PULSAR_URL");

        if(pulsarUrl == null) {
            throw new IOException("Environment variable PULSAR_URL not found");
        }

        try(PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build()) {

            List<String> pvList = loadPvsConfig();

            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topics(pvList)
                    .subscriptionName(UUID.randomUUID().toString())
                    .subscribe();

            while(true) {
                Message msg = consumer.receive();

                System.out.println(msg.getTopicName() + ": " + new String(msg.getData()));

                consumer.acknowledge(msg);
            }

        }
    }

    private List<String> loadPvsConfig() throws IOException {
        List<String> pvList = new ArrayList<>();
        Properties props = new Properties();

        try (InputStream propStream
                     = Subscriber.class.getClassLoader().getResourceAsStream(
                "pvs.properties")) {
            if (propStream == null) {
                throw new IOException(
                        "File Not Found; Configuration File: pvs.properties");
            }

            props.load(propStream);

            String pvsCsv = (String)props.get("PVS_CSV");

            if(pvsCsv != null) {
                String[] pvs = pvsCsv.split(",");
                for(String pv: pvs) {
                    pv = pv.trim();

                    if(!pv.isEmpty()) {
                        System.out.println("Loading PV from config: " + pv);
                        pvList.add(pv);
                    }
                }
            }
        }

        return pvList;
    }

    public static void main(String[] args) throws IOException {
        Subscriber sub = new Subscriber();

        sub.start();
    }
}
