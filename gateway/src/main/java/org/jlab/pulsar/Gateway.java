package org.jlab.pulsar;

import com.cosylab.epics.caj.CAJChannel;
import com.cosylab.epics.caj.CAJContext;
import com.cosylab.epics.caj.CAJMonitor;
import gov.aps.jca.CAException;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.Monitor;
import gov.aps.jca.TimeoutException;
import gov.aps.jca.configuration.DefaultConfiguration;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.event.MonitorEvent;
import gov.aps.jca.event.MonitorListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import javax.management.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Gateway implements GatewayMBean {

    private static final Logger LOGGER = Logger.getLogger(Gateway.class.getName());

    private PulsarClient client = null;
    private CAJContext context = null;
    private transient boolean running = true;
    private static final JCALibrary JCA_LIBRARY = JCALibrary.getInstance();
    private static final DefaultConfiguration CAJ_CONFIG = new DefaultConfiguration("config");

    Map<String, Topic> pvs = new HashMap<>();

    public void start() throws IOException, CAException, TimeoutException {
        String pulsarUrl = System.getenv("PULSAR_URL");

        if(pulsarUrl == null) {
            throw new IOException("Environment variable PULSAR_URL not found");
        }

        String epicsAddrList = System.getenv("EPICS_CA_ADDR_LIST");

        if(epicsAddrList == null) {
            throw new IOException("Environment variable EPICS_CA_ADDR_LIST not found");
        }

        LOGGER.log(Level.INFO,"PULSAR_URL: " + pulsarUrl);
        LOGGER.log(Level.INFO, "EPICS_CA_ADDR_LIST: " + epicsAddrList);

        try(PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build()) {

            this.client = client;

            CAJ_CONFIG.setAttribute("class", JCALibrary.CHANNEL_ACCESS_JAVA);
            CAJ_CONFIG.setAttribute("auto_addr_list", "false");
            CAJ_CONFIG.setAttribute("addr_list", epicsAddrList);

            try {
                context = (CAJContext) JCA_LIBRARY.createContext(CAJ_CONFIG);

                loadPvsConfig();

                while (running) {
                    synchronized (this) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            // Wake up and look around!
                        }
                    }
                }
            } finally {
                if(context != null) {
                    try {
                        context.destroy();
                    } catch(Exception e) {
                        System.err.println("Unable to destroy CAJ context");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        synchronized (this) {
            this.notify();
        }
    }

    @Override
    public synchronized void add(String pv) throws PulsarClientException, CAException, TimeoutException {
        Topic topic = pvs.get(pv);

        if(topic == null) {

            System.out.println("Adding pv: " + pv);

            CAJChannel channel = (CAJChannel) context.createChannel(pv);
            context.pendIO(2.0);
            CAJMonitor monitor = (CAJMonitor) channel.addMonitor(Monitor.VALUE);

            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(pv)
                    .create();

            monitor.addMonitorListener(new MonitorListener() {
                @Override
                public void monitorChanged(MonitorEvent ev) {
                    try {
                        producer.send(dbrToString(ev.getDBR()));
                    } catch(PulsarClientException e) {
                        System.err.println("Unable to send message");
                        e.printStackTrace();
                    }
                }
            });

            context.pendIO(2.0);

            pvs.put(pv, new Topic(producer, channel));
        }
    }

    @Override
    public synchronized void remove(String pv) throws PulsarClientException, CAException {
        Topic topic = pvs.remove(pv);

        if(topic != null) {
            try {
                topic.producer.close();
            } catch(Exception e) {
                e.printStackTrace();
            }

            topic.channel.destroy(true);
        }
    }

    @Override
    public synchronized List<String> list() {
        return new ArrayList<>(pvs.keySet());
    }

    public static void main(String[] args) throws IOException, MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException, CAException, TimeoutException {
        Gateway gateway = new Gateway();

        setupJMX(gateway);

        gateway.start();
    }

    private void loadPvsConfig() throws IOException, TimeoutException, CAException {
        Properties props = new Properties();

        try (InputStream propStream
                     = Gateway.class.getClassLoader().getResourceAsStream(
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
                        add(pv);
                    }
                }
            }
        }
    }

    private String dbrToString(DBR dbr) {
        String result = null;
            try {
                if (dbr.isDOUBLE()) {
                    double value = ((gov.aps.jca.dbr.DOUBLE) dbr).getDoubleValue()[0];
                    if (Double.isFinite(value)) {
                        result = String.valueOf(value);
                    } else if (Double.isNaN(value)) {
                        result = "NaN";
                    } else {
                        result = "Infinity";
                    }
                } else if (dbr.isFLOAT()) {
                    float value = ((gov.aps.jca.dbr.FLOAT) dbr).getFloatValue()[0];
                    if (Float.isFinite(value)) {
                        result = String.valueOf(value);
                    } else if (Float.isNaN(value)) {
                        result = "NaN";
                    } else {
                        result = "Infinity";
                    }
                } else if (dbr.isINT()) {
                    int value = ((gov.aps.jca.dbr.INT) dbr).getIntValue()[0];
                    result = String.valueOf(value);
                } else if (dbr.isSHORT()) {
                    short value = ((gov.aps.jca.dbr.SHORT) dbr).getShortValue()[0];
                    result = String.valueOf(value);
                } else if (dbr.isENUM()) {
                    short value = ((gov.aps.jca.dbr.ENUM) dbr).getEnumValue()[0];
                    result = String.valueOf(value);
                } else if (dbr.isBYTE()) {
                    byte value = ((gov.aps.jca.dbr.BYTE) dbr).getByteValue()[0];
                    result = String.valueOf(value);
                } else {
                    String value = ((gov.aps.jca.dbr.STRING) dbr).getStringValue()[0];
                    result = String.valueOf(value);
                }
            } catch (Exception e) {
                System.err.println("Unable to create JSON from value: " + e);
                dbr.printInfo();
            }

            return result;
    }

    static void setupJMX(Gateway gateway) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        // JMX management - for shutting the server down cleanly and other management tasks
        GatewayMBean manager = gateway;
        MBeanServer mbserver = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("org.jlab:type=EpicsToPulsarGateway");
        mbserver.registerMBean(manager, name);
    }

    class Topic {
        Topic(Producer<String> producer, CAJChannel channel) {
            this.producer = producer;
            this.channel = channel;
        }

        public Producer<String> producer = null;
        public CAJChannel channel = null;
    }
}
