package com.github.eslab;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FakeStore implements Store {

    private ListMultimap<UUID, byte[]> store = ArrayListMultimap.create();
    private final Serializer serializer;

    public FakeStore(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public void store(Event event) {
        store.put(event.getAggregateId(), serializer.serialize(event));
    }

    @Override
    public List<Event> loadEventStream(UUID aggregateId) {
        List<byte[]> bytes = store.get(aggregateId);
        List<Event> result = new ArrayList<>();
        for (byte[] b : bytes) {
            result.add(serializer.deserialize(b, null));
        }
        return result;
    }

    @Override
    public void close() throws Exception {

    }
}
