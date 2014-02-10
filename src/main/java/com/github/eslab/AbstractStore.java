package com.github.eslab;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractStore implements Store {

    protected final Map<Class, Serializer> serializersByClass;
    protected final Map<Class, Long> fingerprintByClass;
    protected final Map<String, Serializer> serializersByRecordName;

    protected final Map<Long, Schema> schemaByFingerprint = new HashMap<>();

    public AbstractStore(Collection<Serializer> serializers) {

        Map<Class, Serializer> serializerMap = new HashMap<>();
        Map<String, Serializer> serializerByRecordMap = new HashMap<>();
        Map<Class, Long> fingerprintMap = new HashMap<>();
        for(Serializer serializer : serializers) {
            serializerMap.put(serializer.getEventClass(), serializer);
            Schema schema = serializer.getSchema();
            serializerByRecordMap.put(schema.getFullName(), serializer);
            long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
            fingerprintMap.put(serializer.getEventClass(), fingerprint);
        }

        serializersByClass
                = new ImmutableMap.Builder<Class, Serializer>().putAll(serializerMap).build();
        serializersByRecordName
                = new ImmutableMap.Builder<String, Serializer>().putAll(serializerByRecordMap).build();
        fingerprintByClass
                = new ImmutableMap.Builder<Class, Long>().putAll(fingerprintMap).build();

    }

    @Override
    public final void store(Event event) {
        checkNotNull(event);
        Serializer serializer = serializersByClass.get(event.getClass());
        byte[] bytes = serializer.serialize(event);

        store(
                new Key(
                        event.getAggregateId(),
                        event.getSequenceNumber(),
                        fingerprintByClass.get(event.getClass())
                )
                , bytes
        );
    }

    protected abstract void store(Key key, byte[] data);

    protected Event deserializeEvent(byte[] data, long fingerprint) {
        Schema schema = schemaByFingerprint.get(fingerprint);
        if (schema == null) {
            schema = loadSchema(fingerprint);
            schemaByFingerprint.put(fingerprint, schema);
        }
        Serializer serializer = serializersByRecordName.get(schema.getFullName());
        return serializer.deserialize(data, schema);
    }

    protected abstract Schema loadSchema(long fingerprint);
}
