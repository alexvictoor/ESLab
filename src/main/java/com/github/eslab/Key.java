package com.github.eslab;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.UUID;

class Key {

    public final UUID id;

    public final long sequence;
    public final long schemaFingerprint;

    public Key(UUID id) {
        this.id = id;
        this.sequence = 0L;
        this.schemaFingerprint = 0L;
    }

    public Key(UUID id, long sequence, long schemaFingerprint) {
        this.id = id;
        this.sequence = sequence;
        this.schemaFingerprint = schemaFingerprint;
    }

    public static Key deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        LongBuffer longBuffer = buffer.asLongBuffer();
        UUID id = new UUID(longBuffer.get(), longBuffer.get());
        return new Key(id, longBuffer.get(), longBuffer.get());
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        LongBuffer longBuffer = buffer.asLongBuffer();
        longBuffer.put(id.getMostSignificantBits());
        longBuffer.put(id.getLeastSignificantBits());
        longBuffer.put(sequence);
        longBuffer.put(schemaFingerprint);

        return buffer.array();
    }

    @Override
    public String toString() {
        return "Key{" +
                "id=" + id +
                ", sequence=" + sequence +
                ", schemaFingerprint=" + schemaFingerprint +
                '}';
    }
}
