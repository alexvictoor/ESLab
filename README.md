ESLab as Event Sourcing Lab
===========================

ESLab in a nutshell
-------------------

ESLab is a realy simple event store relying on two things:

1. Avro, a very efficient serialization tool with a killer feature for event sourcing: [Schema Resolution](http://avro.apache.org/docs/1.7.5/spec.html#Schema+Resolution)
2. Java B-Tree on filesystem to acually persist events.  

B-Tree implementations are pluggable and you can choose between:
- Java BerkeleyDB aka Sleepycat
- LevelDB, java version or the original one wrapped with JNI
- MapDB, an interesting open source project that leverage on memory mapped files. 

The purpose of ESLAb is to demonstrate how easy is to build an event store. You can also use this project to compare these different B-Tree implementations. Also feel free to use it on your projects, to contribute to the code.

Event sourcing and Avro schema resolution
-----------------------------------------

TBD


Bench
-------

TBD



