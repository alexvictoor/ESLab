package com.github.eslab;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class MapDbStore extends AbstractStore {

    private final ConcurrentNavigableMap<Fun.Tuple3, byte[]> events;
    private final ConcurrentNavigableMap<Long, byte[]> schemas;
    private final DB db;


    public MapDbStore(String path, Collection<Serializer> serializers) {
        super(serializers);
        db = DBMaker.newFileDB(new File(path, "mapdb.store"))
                .closeOnJvmShutdown()
                .asyncWriteEnable()
                .make();

        events = db.createTreeMap("events").keySerializer(BTreeKeySerializer.TUPLE3).make();
        schemas = db.createTreeMap("schemas").make();

        for(Serializer serializer : serializers) {
            Schema schema = serializer.getSchema();
            long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
            byte[] data = schemas.get(fingerprint);
            if (data == null) {
                schemas.put(fingerprint, schema.toString().getBytes());
            }
        }
    }

    @Override
    public void close() throws Exception {
        db.close();
    }

    @Override
    protected void store(Key key, byte[] data) {
        events.put(Fun.t3(key.id, key.sequence, key.schemaFingerprint), data);
    }

    @Override
    protected Schema loadSchema(long fingerprint) {
        byte[] data = schemas.get(fingerprint);
        if (data == null) {
            throw new RuntimeException("Schema not found for fingerprint: " + fingerprint);
        }
        return new Schema.Parser().parse(new String(data));
    }


    @Override
    public List<Event> loadEventStream(UUID aggregateId) {
        checkNotNull(aggregateId);
        List<Event> result = new ArrayList<>();
        Set<Map.Entry<Fun.Tuple3, byte[]>> entries = events.subMap(Fun.t3(aggregateId, 0L, null), Fun.t3(aggregateId, Fun.HI, Fun.HI)).entrySet();
        for (Map.Entry<Fun.Tuple3, byte[]> entry : entries) {
            result.add(deserializeEvent(entry.getValue(), (Long)entry.getKey().c));
        }
        return result;
    }
}
