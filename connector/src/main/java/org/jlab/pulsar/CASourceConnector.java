package org.jlab.pulsar;

import gov.aps.jca.dbr.DBR;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CASourceConnector<String> extends PushSource<String> {
    private static final Logger LOG = LoggerFactory.getLogger(CASourceConnector.class);

    private Map<String, DBR> latest = new ConcurrentHashMap<>();
    private CASourceConfig instanceConfig = null;
    private volatile boolean running = false;
    private Thread runnerThread;

    /**
     * Open connector with configuration.
     *
     * @param config        initialization config
     * @param sourceContext environment where the source connector is running
     * @throws Exception IO type exceptions when opening a connector
     */
    @Override
    public void open(Map<java.lang.String, Object> config, SourceContext sourceContext) throws Exception {
        instanceConfig = CASourceConfig.load(config, sourceContext);

        this.start();
        running = true;
    }

    public void start() {
        runnerThread = new Thread(() -> {
            LOG.info("Starting CA source");

            while (running) {
                Set<String> updatedPvs = latest.keySet();

                for (String pv: updatedPvs) {
                    CARecord<String> record = new CARecord<>();
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

    }

    private static class CARecord<String> implements Record<String> {

        /**
         * Retrieves the actual data of the record.
         *
         * @return The record data
         */
        @Override
        public String getValue() {
            return null;
        }
    }
}
