# epics2pulsar
Gateway to pump EPICS PVs into a Pulsar Message Broker

## Build
This project uses the [Gradle](https://gradle.org) build tool to automatically download dependencies and build project from source:
````
git clone https://github.com/JeffersonLab/epics2pulsar
cd epics2pulsar
gradlew build -x test
````
*Note:* Gradle can't download dependencies if you're behind a firewall

## Configuration
This application uses the Channel Access for Java library. It requires a working [EPICS](https://epics-controls.org) channel access environment with the environment variable EPICS_CA_ADDR_LIST set.
