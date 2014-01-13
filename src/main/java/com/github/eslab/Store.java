package com.github.eslab;

import java.util.List;
import java.util.UUID;

public interface Store extends AutoCloseable {

    void store(Event event);

    List<Event> loadEventStream(UUID aggregateId);
}
