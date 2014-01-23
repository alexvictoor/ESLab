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

An event store needs a serialization mechanism to serialize events. Once an events has been serialized, if the related java classes has evolved, the event store should still be able to deserialized it. That is why you need a flexible serializeer to do event sourcing, something really different from java standard serialization mechanism. We could use json or xml with something like XStream but such text based serialization are verbose ans hence disk expensive and not very efficient. 

Apache Avro is a serialization tool that is part of the Hadoop ecosystem and is now a top level Apache project. Avro has performances comparable to products such as Google protocol buffer but unlike Google protocol buffer, Avro has been built with flexibility and dynamic languages in mind. If you do not know Avro you should definitely check out the [Avros's Getting Started page](http://avro.apache.org/docs/1.7.5/gettingstartedjava.html).
Unkike other tools, Avro does not require code generation. Like other tools, the structure of data serialized is specified by a schema, but unlike the other ones you can use one schema for serializing an object and an other one to deserialize the same object. This is a killer feature for event sourcong since your event classes migh evolve, fields might be added/removed/renamed without any big impact on your event store. For detailed information on this wonderful feature check out the [Avro's Schema Resolution documentation](http://avro.apache.org/docs/1.7.5/spec.html#Schema+Resolution).

How ESLab leverage on Avro
--------------------------

TBD



Bench
-------

TBD



