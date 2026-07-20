/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.sink.source.engine;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.pixelsdb.pixels.sink.source.engine.PixelsDebeziumConsumer.RecordType.ROW;
import static io.pixelsdb.pixels.sink.source.engine.PixelsDebeziumConsumer.RecordType.TOMBSTONE;
import static io.pixelsdb.pixels.sink.source.engine.PixelsDebeziumConsumer.RecordType.TRANSACTION;
import static io.pixelsdb.pixels.sink.source.engine.PixelsDebeziumConsumer.RecordType.UNKNOWN_CONTROL;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PixelsDebeziumConsumerTest
{
    private static final String PREFIX = "mysql-cdc";
    private static final String ROW_TOPIC = PREFIX + ".cdc_verify.binlog_test";
    private static final String TRANSACTION_TOPIC = PREFIX + ".transaction";
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("connector", Schema.STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
            .field("file", Schema.STRING_SCHEMA)
            .field("pos", Schema.INT64_SCHEMA)
            .field("row", Schema.INT32_SCHEMA)
            .build();
    private static final Schema TRANSACTION_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();

    @Test
    void shouldClassifyMySqlRowOperations()
    {
        assertEquals(ROW, classify(rowRecord("c", null, row(1), "gtid:3", 1)));
        assertEquals(ROW, classify(rowRecord("u", row(1), row(1), "gtid:3", 2)));
        assertEquals(ROW, classify(rowRecord("d", row(1), null, "gtid:3", 3)));
        assertEquals(ROW, classify(rowRecord("r", null, row(1), "", 0)));
    }

    @Test
    void shouldTreatTwoRowsFromOneWriteRowsAsIndependentEvents()
    {
        SourceRecord first = rowRecord("c", null, row(1), "gtid:4", 1);
        SourceRecord second = rowRecord("c", null, row(2), "gtid:4", 2);

        assertEquals(ROW, classify(first));
        assertEquals(ROW, classify(second));
        assertEquals("gtid:4",
                ((Struct) ((Struct) first.value()).get("transaction")).getString("id"));
        assertEquals("gtid:4",
                ((Struct) ((Struct) second.value()).get("transaction")).getString("id"));
    }

    @Test
    void shouldKeepReplaceDeleteAndInsertInOneTransaction()
    {
        SourceRecord delete = rowRecord("d", row(2), null, "gtid:7", 1);
        SourceRecord insert = rowRecord("c", null, row(2), "gtid:7", 2);

        assertEquals(ROW, classify(delete));
        assertEquals(ROW, classify(insert));
    }

    @Test
    void shouldClassifyTransactionBoundaries()
    {
        assertEquals(TRANSACTION, classify(transactionRecord("BEGIN", "gtid:5")));
        assertEquals(TRANSACTION, classify(transactionRecord("END", "gtid:5")));
        assertEquals(UNKNOWN_CONTROL, classify(transactionRecord("COMMIT", "gtid:5")));
    }

    @Test
    void shouldSkipControlEvents()
    {
        assertEquals(TOMBSTONE, classify(sourceRecord(ROW_TOPIC, null, null)));
        assertEquals(UNKNOWN_CONTROL, classify(sourceRecord(
                PREFIX + ".signal", SchemaBuilder.struct()
                        .field("type", Schema.STRING_SCHEMA).build(),
                new Struct(SchemaBuilder.struct()
                        .field("type", Schema.STRING_SCHEMA).build()).put("type", "x"))));
    }

    @Test
    void shouldRejectMalformedRows()
    {
        assertEquals(UNKNOWN_CONTROL,
                classify(rowRecord("u", null, row(1), "gtid:6", 1)));
        assertEquals(UNKNOWN_CONTROL,
                classify(rowRecord("d", null, null, "gtid:6", 1)));
    }

    private PixelsDebeziumConsumer.RecordType classify(SourceRecord record)
    {
        return PixelsDebeziumConsumer.classify(record, TRANSACTION_TOPIC);
    }

    private SourceRecord rowRecord(
            String op, Struct before, Struct after, String transactionId, long order)
    {
        Schema transactionSchema = transactionSchema();
        Struct transaction = transactionId.isEmpty() ? null : new Struct(transactionSchema)
                .put("id", transactionId)
                .put("total_order", order)
                .put("data_collection_order", order);
        Schema sourceSchema = sourceSchema();
        Struct source = new Struct(sourceSchema)
                .put("connector", "mysql")
                .put("db", "cdc_verify")
                .put("table", "binlog_test")
                .put("gtid", transactionId.isEmpty() ? null : transactionId)
                .put("file", "binlog.000004")
                .put("pos", 152L)
                .put("row", (int) order);
        Schema envelopeSchema = SchemaBuilder.struct()
                .name("mysql-cdc.cdc_verify.binlog_test.Envelope")
                .field("before", rowSchema())
                .field("after", rowSchema())
                .field("source", sourceSchema)
                .field("transaction", transactionSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", after)
                .put("source", source)
                .put("transaction", transaction)
                .put("op", op);
        return sourceRecord(ROW_TOPIC, envelopeSchema, envelope);
    }

    private SourceRecord transactionRecord(String status, String id)
    {
        Schema collectionSchema = SchemaBuilder.struct()
                .field("data_collection", Schema.STRING_SCHEMA)
                .field("event_count", Schema.INT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("status", Schema.STRING_SCHEMA)
                .field("id", Schema.STRING_SCHEMA)
                .field("event_count", Schema.OPTIONAL_INT64_SCHEMA)
                .field("data_collections", SchemaBuilder.array(collectionSchema).optional().build())
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("status", status)
                .put("id", id)
                .put("event_count", status.equals("END") ? 4L : null)
                .put("data_collections", null)
                .put("ts_ms", 1750000000000L);
        return sourceRecord(TRANSACTION_TOPIC, schema, value);
    }

    private SourceRecord sourceRecord(String topic, Schema schema, Object value)
    {
        return new SourceRecord(
                Map.of("server", PREFIX),
                Map.of("file", "binlog.000004", "pos", 152L),
                topic,
                null,
                null,
                schema,
                value);
    }

    private Struct row(long id)
    {
        return new Struct(rowSchema()).put("id", id).put("name", "row-" + id);
    }

    private Schema rowSchema()
    {
        return ROW_SCHEMA;
    }

    private Schema sourceSchema()
    {
        return SOURCE_SCHEMA;
    }

    private Schema transactionSchema()
    {
        return TRANSACTION_SCHEMA;
    }
}
