# epics2pulsar
Gateway to pump EPICS PVs into a Pulsar Message Broker

## Build
This project uses the [Gradle](https://gradle.org) build tool to automatically download dependencies and build the project from source:
````
git clone https://github.com/JeffersonLab/epics2pulsar
cd epics2pulsar
gradlew build -x test
````
__Note:__ If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source 

__Note:__ A firewall may prevent Gradle from downloading packages and dependencies from the Internet.   You may need to setup a [proxy](https://github.com/JeffersonLab/jmyapi/wiki/JLab-Proxy).   

## Configuration
This application uses the Channel Access for Java library. It requires a working [EPICS](https://epics-controls.org) channel access environment with the environment variable EPICS_CA_ADDR_LIST set.
