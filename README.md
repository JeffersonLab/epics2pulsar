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
