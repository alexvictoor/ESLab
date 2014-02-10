package com.github.eslab;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.fest.assertions.Assertions.assertThat;

public class AbstractStoreTest {

    private AbstractStore store;

    @Before
    public void setUp() {
        Collection<Serializer> serializers = Lists.newArrayList();
        serializers.add(new SimpleSerializer());
        store = new InMemoryStore(serializers);
    }

    @Test
    public void should_be_able_to_retrieve_stored_events() {
        // given
        UUID id = UUID.randomUUID();
        // when
        store.store(new SimpleEvent(id, 36, 0));
        // then
        List<Event> events = store.loadEventStream(id);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getAggregateId()).isEqualTo(id);
        assertThat(events.get(0).getSequenceNumber()).isEqualTo(0);
        assertThat(events.get(0).getTimestamp()).isEqualTo(36);
    }



    public static class InMemoryStore extends AbstractStore {

        private Map<Key, byte[]> events = new HashMap<>();
        private Map<Long, Schema> schemas = new HashMap<>();

        public InMemoryStore(Collection<Serializer> serializers) {
            super(serializers);
            for(Serializer serializer : serializers) {
                Schema schema = serializer.getSchema();
                long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
                schemas.put(fingerprint, schema);
            }
        }

        @Override
        protected void store(Key key, byte[] data) {
            events.put(key, data);
        }

        @Override
        protected Schema loadSchema(long fingerprint) {
            return schemas.get(fingerprint);
        }

        @Override
        public List<Event> loadEventStream(UUID aggregateId) {
            Set<Map.Entry<Key, byte[]>> entries = events.entrySet();
            List<Event> events = new ArrayList<>();
            for(Map.Entry<Key, byte[]> entry : entries) {
                events.add(deserializeEvent(entry.getValue(), entry.getKey().schemaFingerprint));
            }
            return events;
        }

        @Override
        public void close() throws Exception {
            // not needed
        }
    }
}
