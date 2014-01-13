package com.github.eslab;

import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: FamilleVictoor
 * Date: 21/12/13
 * Time: 22:20
 * To change this template use File | Settings | File Templates.
 */
public interface Event {

    UUID getAggregateId();

    long getTimestamp();

    long getSequenceNumber();

}
