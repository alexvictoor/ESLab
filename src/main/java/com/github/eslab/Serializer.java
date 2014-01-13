package com.github.eslab;

import org.apache.avro.Schema;

public interface Serializer<T extends Event> {

    Class getEventClass();

    Schema getSchema();

    T deserialize(byte[] bytes, Schema writerSchema);

    byte[] serialize(T event);


}
