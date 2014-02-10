package com.github.eslab;


import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class LdbStoreSupport extends AbstractStore {

    final static Logger logger = LoggerFactory.getLogger(LdbStoreSupport.class);

    private final DB eventDatabase;
    private final DB schemaDatabase;

    public LdbStoreSupport(String path, Collection<Serializer> serializers, DBFactory factory) {
        super(serializers);
        Options options = new Options();
        options.createIfMissing(true);
        try {
            eventDatabase = factory.open(new File(path, "events.leveldb"), options);
            schemaDatabase = factory.open(new File(path, "schemas.leveldb"), options);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    close();
                } catch (IOException e) {
                    logger.error("Error while closing LevelDB file", e);
                }
            }

        });

        for(Serializer serializer : serializers) {
            Schema schema = serializer.getSchema();
            long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
            byte[] fingerprintData = ByteBuffer.allocate(8).putLong(fingerprint).array();
            byte[] data = schemaDatabase.get(fingerprintData);
            if (data == null) {
                schemaDatabase.put(fingerprintData, schema.toString().getBytes());
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            schemaDatabase.close();
        } finally {
            eventDatabase.close();
        }
    }

    @Override
    protected void store(Key key, byte[] data) {
        eventDatabase.put(key.toBytes(), data);
    }

    @Override
    protected Schema loadSchema(long fingerprint) {
        byte[] fingerprintData = ByteBuffer.allocate(8).putLong(fingerprint).array();
        byte[] data = schemaDatabase.get(fingerprintData);
        if (data == null) {
            throw new RuntimeException("Schema not found for fingerprint: " + fingerprint);
        }
        return new Schema.Parser().parse(new String(data));
    }

    @Override
    public List<Event> loadEventStream(UUID aggregateId) {
        checkNotNull(aggregateId);
        DBIterator iterator = null;

        try {
            iterator = eventDatabase.iterator();
            iterator.seek(new Key(aggregateId).toBytes());

            List<Event> events = new ArrayList<>();

            boolean keepMoving = true;
            while (keepMoving && iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                Key key = Key.deserialize(entry.getKey());
                if (!aggregateId.equals(key.id)) {
                    keepMoving = false;
                }  else {
                    events.add(deserializeEvent(entry.getValue(), key.schemaFingerprint));
                }
            }
            return events;
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (IOException e) {
                    logger.error("Error while closing LevelDB iterator", e);
                }
            }
        }
    }
}