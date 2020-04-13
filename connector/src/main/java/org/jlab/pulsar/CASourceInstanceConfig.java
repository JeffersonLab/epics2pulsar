package org.jlab.pulsar;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class CASourceInstanceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String addrs;
    private List<String> pvs;

    public CASourceInstanceConfig(String addrs, List<String> pvs) {
        this.addrs = addrs;
        this.pvs = pvs;
    }

    public List<String> getPvs() {
        return pvs;
    }

    public String getAddrs() {
        return addrs;
    }
}
