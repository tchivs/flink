# Flink Integration API

The purpose of this project is to isolate job-submission-service (JSS) and autopilot-controller from Flink Versions.

It includes interfaces used by JSS, mostly
1. To translate CompiledPlans into JobGraphs
2. Submit them to the Flink Cluster

Between calling (1) and (2), JSS calls autopilot-controller to obtain the initial resource requirements and then sets them on the JobGraph.

The interfaces are to be implemented for each supported Flink version (and downloaded by JSS at run time):
- The bundle for a version should include all the required dependencies (e.g. Flink Clients, Connectors, Scala, etc.).
- Eventually, the implementations will be owned by SQL Service Team to tune JobGraph generation.

For M0.1, the implementation will be bundled into this module.
