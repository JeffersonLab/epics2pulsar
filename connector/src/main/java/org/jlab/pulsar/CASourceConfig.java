package org.jlab.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Data
@Accessors(chain = true)
public class CASourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A space separated list of EPICS server addresses")
    private String addrs;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "A space separated list of EPICS PV names")
    private String pvs;

    public static CASourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), CASourceConfig.class);
    }

    public static CASourceConfig load(Map<String, Object> map, SourceContext sourceContext) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), CASourceConfig.class);
    }

    public CASourceInstanceConfig getInstanceConfig(Map<String, Object> config, SourceContext sourceContext) {
        int maxTasks = sourceContext.getNumInstances();

        String epicsAddrList = addrs;
        Set<String> pvsSet = new HashSet<>(Arrays.asList(pvs.split("\\s+")));

        List<CASourceInstanceConfig> configs = new ArrayList<>();

        // Default case is same number of pvs as tasks so each task has one
        int pvsPerTask = 1;
        int remainder = 0;

        if(pvsSet.size() > maxTasks) {
            pvsPerTask = pvsSet.size() / maxTasks;
            remainder = pvsSet.size() % maxTasks;
        } else if(pvsSet.size() < maxTasks) {
            //maxTasks = pvs.size(); // Reduce number of tasks as not enough work to go around!

            // Unlike with Kafka, we can't reduce # of tasks if requested parallelism is too high
            throw new RuntimeException("Requested parallelism is higher than amount of work to divide up");
        }

        List<String> all = new ArrayList<>(pvsSet);

        int fromIndex = 0;
        int toIndex = pvsPerTask + remainder;

        // Always at least one - maxTasks ignored if < 1;  Also first one takes remainder
        if(toIndex > 0) {
            appendSubsetList(configs, all, fromIndex, toIndex);
        }

        fromIndex = toIndex;
        toIndex = toIndex + pvsPerTask;

        for(int i = 1; i < maxTasks; i++) {
            appendSubsetList(configs, all, fromIndex, toIndex);

            fromIndex = toIndex;
            toIndex = toIndex + pvsPerTask;
        }

        CASourceInstanceConfig instanceConf = configs.get(sourceContext.getInstanceId());

        return instanceConf;
    }

    private void appendSubsetList(List<CASourceInstanceConfig> configs, List<String> all, int fromIndex, int toIndex) {
        List<String> subset = all.subList(fromIndex, toIndex);

        CASourceInstanceConfig config = new CASourceInstanceConfig(addrs, subset);

        configs.add(config);
    }
}
