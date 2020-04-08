package org.jlab.pulsar;

import gov.aps.jca.CAException;
import gov.aps.jca.TimeoutException;
import org.apache.commons.cli.*;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.List;

public class AdminCli {

    public static void main(String[] args) throws IOException, MalformedObjectNameException, CAException, TimeoutException {

        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption( "a", "add", true, "add pv" );
        options.addOption( "r", "remove", true, "remove pv" );
        options.addOption( "l", "list", false, "list pvs" );
        options.addOption( "s", "shutdown", false, "shutdown gateway" );

        CommandLine line = null;

        try {
             line = parser.parse(options, args);
        } catch(ParseException e) {
            System.out.println("Unable to parse arguments: " + e.getMessage());
        }

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi");
        try (JMXConnector jmxc = JMXConnectorFactory.connect(url, null)) {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            ObjectName mbeanName = new ObjectName("org.jlab:type=EpicsToPulsarGateway");
            GatewayMBean mbeanProxy = JMX.newMBeanProxy(mbsc, mbeanName, GatewayMBean.class, true);

            System.out.println("Connected to Gateway");

            if(line.hasOption("a")) {
                mbeanProxy.add(line.getOptionValue("a"));
            }

            if(line.hasOption("r")) {
                mbeanProxy.remove(line.getOptionValue("r"));
            }

            if(line.hasOption("l")) {
                List<String> pvs = mbeanProxy.list();

                System.out.println("pvs:");

                for (String pv : pvs) {
                    System.out.println(pv);
                }
            }

            if(line.hasOption("s")) {
                mbeanProxy.stop();
            }
        }
    }
}
