package org.jlab;

import gov.aps.jca.CAException;
import gov.aps.jca.TimeoutException;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;

public interface EpicsToPulsarGatewayMBean {
    public void stop();
    public void add(String pv) throws PulsarClientException, CAException, TimeoutException;
    public void remove(String pv) throws PulsarClientException, CAException;
    public List<String> list();
}
