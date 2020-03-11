# epics2pulsar
Gateway to pump [EPICS](https://epics-controls.org) PVs into a [Pulsar](https://pulsar.apache.org) Message Broker

## Build
This project uses the [Gradle](https://gradle.org) build tool to automatically download dependencies and build the project from source:
````
git clone https://github.com/JeffersonLab/epics2pulsar
cd epics2pulsar
gradlew build -x test
````
__Note:__ If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source 

__Note:__ A firewall may prevent Gradle from downloading packages and dependencies from the Internet.   You may need to setup a [proxy](https://github.com/JeffersonLab/jmyapi/wiki/JLab-Proxy).   

## Configure
This application uses the Channel Access for Java (CAJ) library. It requires a working EPICS channel access environment with the environment variable _EPICS_CA_ADDR_LIST_ set.

The list of PVs to monitor is read from the file _pvs.properties_.  The Java [Properties](https://en.wikipedia.org/wiki/.properties) file contains a single key named _PVS_CSV_, which is a comma separated values list of PV names.

The Pulsar broker to use is configured via an environment variable named _PULSAR_URL_.

## Run
### Gateway
Launch the gateway with:
````
gradle :gateway:run
````
### Admin Interface
Launch the admin command line interface (CLI) to shutdown the gateway with:
````
gradle :admin:run --args="-s"
````
All CLI Commands:

| Command | Description |
|---------|--------|
| -s | Shutdown Gateway |
| -l | List monitored PVs |
| -a [pv] | Add PV |
| -r [pv] | Remove PV | 

### Subscribers
Launch one or more subscribers with:
````
gradle :subscriber:run
````

__Note:__ If you deploy this project's distributable package to a server without Gradle you can use the scripts generated by the build to run the gateway and admin applications.  The scripts are found in the build directories and are named gateway.sh, admin.sh, and subscriber.sh.  There are also Windows .bat versions as well. 

## See Also
### Related Projects (External)
   - [EPICS to Kafka (ESS)](https://github.com/ess-dmsc/forward-epics-to-kafka)
   - [Control System Studio Phoebus Alarms over Kafka](https://github.com/ControlSystemStudio/phoebus/tree/master/app/alarm)
   - [Kafka at CERN](http://icalepcs2019.vrws.de/papers/mompl010.pdf)
   
### Research / Prototypes (Internal)   
   - [pulsar-alarms](https://github.com/JeffersonLab/pulsar-alarms)
   - [kafka-alarms](https://github.com/JeffersonLab/kafka-alarms)
   - [epics2kafka](https://github.com/JeffersonLab/epics2kafka)
