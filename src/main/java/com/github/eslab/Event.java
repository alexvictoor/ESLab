package com.github.eslab;

import java.util.UUID;

public interface Event {

    UUID getAggregateId();

    long getTimestamp();

    long getSequenceNumber();

}
