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
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CASourceConnector extends PushSource<String> {
    private static final Logger LOG = LoggerFactory.getLogger(CASourceConnector.class);

    private static final Logger log = LoggerFactory.getLogger(CASourceConnector.class);
    private static final JCALibrary JCA_LIBRARY = JCALibrary.getInstance();
    private DefaultConfiguration jcaConfig = new DefaultConfiguration("config");

    private Map<String, DBR> latest = new ConcurrentHashMap<>();
    private CASourceConfig globalConfig = null;
    private CASourceInstanceConfig instanceConfig = null;
    private volatile boolean running = false;
    private Thread runnerThread;
    private CAJContext context;

    /**
     * Open connector with configuration.
     *
     * @param config        initialization config
     * @param sourceContext environment where the source connector is running
     * @throws Exception IO type exceptions when opening a connector
     */
    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        globalConfig = CASourceConfig.load(config, sourceContext);
        instanceConfig =  globalConfig.getInstanceConfig(config, sourceContext);

        if(context == null) {
            createContext();
        }

        this.start();
        running = true;
    }

    public void start() {
        runnerThread = new Thread(() -> {
            LOG.info("Starting CA source");

            while (running) {

                synchronized (runnerThread) {
                    try {
                        runnerThread.wait(100); // Max update frequency of 10Hz
                    } catch(InterruptedException e) {
                        // Do nothing other than continue loop and re-check running boolean
                    }
                }

                Set<String> updatedPvs = latest.keySet();

                for (String pv: updatedPvs) {
                    DBR dbr = latest.remove(pv);
                    String value = dbrToString(dbr);
                    String topic = pv.replaceAll(":", "-"); // Does pulsar restrict colon in topic name?

                    CARecord record = new CARecord(value, topic);
                    consume(record);
                }
            }
        });
        runnerThread.setUncaughtExceptionHandler((t, e) -> LOG.error("[{}] Error while consuming records", t.getName(), e));
        runnerThread.setName("CA Source Thread");
        runnerThread.start();
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        if(context != null) {
            try {
                context.destroy();
            } catch(CAException e) {
                log.error("Failed to destroy CAJContext", e);
            }
        }
        running = false;
        synchronized (runnerThread) {
            runnerThread.notify();
        }
    }

    private void createContext() {

        jcaConfig.setAttribute("class", JCALibrary.CHANNEL_ACCESS_JAVA);
        jcaConfig.setAttribute("auto_addr_list", "false");
        jcaConfig.setAttribute("addr_list", instanceConfig.getAddrs());

        try {
            context = (CAJContext) JCA_LIBRARY.createContext(jcaConfig);

            List<CAJChannel> channels = new ArrayList<>();
            for(String pv: instanceConfig.getPvs()) {
                channels.add((CAJChannel)context.createChannel(pv));
            }

            context.pendIO(2.0);

            for(CAJChannel channel: channels) {
                CAJMonitor monitor = (CAJMonitor) channel.addMonitor(Monitor.VALUE);

                monitor.addMonitorListener(new MonitorListener() {
                    @Override
                    public void monitorChanged(MonitorEvent ev) {
                        latest.put(channel.getName(), ev.getDBR());
                    }
                });
            }

            context.pendIO(2.0);

        } catch(CAException | TimeoutException e) {
            log.error("Error while trying to create CAJContext");
            throw new RuntimeException(e);
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
            System.err.println("Unable to create String from value: " + e);
            dbr.printInfo();
        }

        return result;
    }

    private static class CARecord implements Record<String> {

        private String topic;
        private String value;

        public CARecord(String value, String topic) {
            this.value = value;
            this.topic = topic;
        }

        /**
         * Retrieves the actual data of the record.
         *
         * @return The record data
         */
        @Override
        public String getValue() {
            return value;
        }

        @Override
        public Optional<String> getTopicName() {
            return Optional.of(topic);
        }
    }
}
