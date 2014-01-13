package com.github.eslab;

import java.util.UUID;


public class SimpleEvent implements Event {

    private final UUID id;

    private final long timestamp;

    private final long sequenceNumber;

    public SimpleEvent(UUID id, long timestamp, long sequenceNumber) {
        this.id = id;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public UUID getAggregateId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
