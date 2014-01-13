package com.github.eslab;


import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

public class FatSimpleSerializer implements Serializer<SimpleEvent> {

    public static final Schema SCHEMA;

    static {
        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
                .record("Simple").namespace("example")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("sequence").type().longType().noDefault()
                .name("timestamp").type().longType().noDefault();

        for (int i=0; i<20; i++) {
            builder.name("fatField"+i).type().longType().longDefault(777);
        }
        SCHEMA = builder.endRecord();

    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }

    @Override
    public Class getEventClass() {
        return SimpleEvent.class;
    }

    BinaryDecoder decoder = null;

    @Override
    public SimpleEvent deserialize(byte[] bytes, Schema writerSchema) {
        DatumReader<GenericRecord> datumReader = null;
        if (writerSchema == null) {
            datumReader = new GenericDatumReader<GenericRecord>(SCHEMA);
        } else {
            datumReader = new GenericDatumReader<GenericRecord>(writerSchema, SCHEMA);
        }

        decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
        GenericRecord record = null;
        try {
            record = datumReader.read(null, decoder);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        UUID id = UUID.fromString(((Utf8) record.get("id")).toString());
        long sequence = (Long) record.get("sequence");
        long timestamp = (Long) record.get("timestamp");

        return new SimpleEvent(id, timestamp, sequence);
    }


    BinaryEncoder encoder = null;

    @Override
    public byte[] serialize(SimpleEvent event) {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(SCHEMA);
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("id", event.getAggregateId().toString());
        record.put("sequence", event.getSequenceNumber());
        record.put("timestamp", event.getTimestamp());
        for (int i=0; i<20; i++) {
            record.put("fatField"+i, event.getTimestamp());
        }

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(stream, encoder);
        try {
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return stream.toByteArray();
    }
}
