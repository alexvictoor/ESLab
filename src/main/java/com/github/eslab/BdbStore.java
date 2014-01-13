package com.github.eslab;

import com.sleepycat.je.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class BdbStore extends AbstractStore {

    /** fingerprint -> json schema **/
    private final Database schemaDatabase;

    /** (id, sequence, fingerprint) -> data **/
    private final Database eventDatabase;

    private final Environment environment;



    public BdbStore(String path, Collection<Serializer> serializers) {
        super(serializers);
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);

        environment = new Environment(new File(path), environmentConfig);
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setSortedDuplicates(false);
        databaseConfig.setDeferredWrite(true);
        eventDatabase = environment.openDatabase(null, "EventDatabase", databaseConfig);
        schemaDatabase = environment.openDatabase(null, "SchemaDatabase", databaseConfig);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
              close();
            }
        });

        for(Serializer serializer : serializers) {
            Schema schema = serializer.getSchema();
            long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
            DatabaseEntry keyEntry = new DatabaseEntry(ByteBuffer.allocate(8).putLong(fingerprint).array());
            DatabaseEntry valueEntry = new DatabaseEntry();
            OperationStatus operationStatus = schemaDatabase.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
            if (operationStatus == OperationStatus.NOTFOUND) {
                valueEntry.setData(schema.toString().getBytes());
                schemaDatabase.put(null, keyEntry, valueEntry);
            }
        }
    }

    @Override
    public void close() {
        try {
            schemaDatabase.close();
        } finally {
            try {
                eventDatabase.close();
            } finally {
                environment.close();
            }
        }
    }

    protected void store(Key key, byte[] data) {
        DatabaseEntry keyEntry
                = new DatabaseEntry(key.toBytes());
        DatabaseEntry valueEntry = new DatabaseEntry(data);
        eventDatabase.put(null, keyEntry, valueEntry);
    }

    protected Schema loadSchema(long fingerprint) {
        DatabaseEntry keyEntry = new DatabaseEntry(ByteBuffer.allocate(8).putLong(fingerprint).array());
        DatabaseEntry valueEntry = new DatabaseEntry();
        OperationStatus status = schemaDatabase.get(null, keyEntry, valueEntry, LockMode.DEFAULT);
        if (status == OperationStatus.NOTFOUND) {
            throw new RuntimeException("Schema not found for fingerprint: " + fingerprint);
        }
        return new Schema.Parser().parse(new String(valueEntry.getData()));
    }

    @Override
    public List<Event> loadEventStream(UUID aggregateId) {
        checkNotNull(aggregateId);
        Cursor cursor = null;
        try {
            cursor = eventDatabase.openCursor(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(new Key(aggregateId).toBytes());
            DatabaseEntry searchEntry = new DatabaseEntry();
            OperationStatus operationStatus =  cursor.getSearchKeyRange(keyEntry, searchEntry, LockMode.DEFAULT);

            if (operationStatus == OperationStatus.NOTFOUND) {
                return Collections.emptyList();
            }

            Key key = Key.deserialize(keyEntry.getData());
            if (!aggregateId.equals(key.id)) {
                return Collections.emptyList();
            }
            List<Event> events = new ArrayList<>();
            events.add(deserializeEvent(searchEntry.getData(), key.schemaFingerprint));

            boolean keepMoving = true;
            while (keepMoving && (cursor.getNext(keyEntry, searchEntry, LockMode.DEFAULT) != OperationStatus.NOTFOUND)) {
                key = Key.deserialize(keyEntry.getData());
                if (!aggregateId.equals(key.id)) {
                    keepMoving = false;
                }  else {
                    events.add(deserializeEvent(searchEntry.getData(), key.schemaFingerprint));
                }
            }
            return events;
        } finally {
            if (cursor !=null) {
                cursor.close();
            }
        }
    }
}
