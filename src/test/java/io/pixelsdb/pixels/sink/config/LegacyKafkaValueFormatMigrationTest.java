/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */

package io.pixelsdb.pixels.sink.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyKafkaValueFormatMigrationTest
{
    @Test
    void shouldDefaultToJsonWhenNoFormatOrLegacyKeys()
    {
        assertEquals("json", PixelsSinkConfig.resolveKafkaValueFormat(new Properties()));
    }

    @Test
    void shouldInferJsonFromLegacyDeserializerClass()
    {
        Properties props = new Properties();
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventJsonDeserializer");
        props.setProperty(
                "transaction.topic.value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.TransactionMetadataJsonDeserializer");
        assertEquals("json", PixelsSinkConfig.resolveKafkaValueFormat(props));
    }

    @Test
    void shouldInferAvroFromLegacyDeserializerClass()
    {
        Properties props = new Properties();
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.source.kafka.serde.RowChangeEventAvroDeserializer");
        assertEquals("avro", PixelsSinkConfig.resolveKafkaValueFormat(props));
    }

    @Test
    void shouldPreferExplicitFormatAndIgnoreLegacyKeys()
    {
        Properties props = new Properties();
        props.setProperty("sink.kafka.value.format", "avro");
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventJsonDeserializer");
        assertEquals("avro", PixelsSinkConfig.resolveKafkaValueFormat(props));
    }

    @Test
    void shouldFailFastOnConflictingLegacyFormats()
    {
        Properties props = new Properties();
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventJsonDeserializer");
        props.setProperty(
                "transaction.topic.value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.TransactionMetadataAvroDeserializer");
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PixelsSinkConfig.resolveKafkaValueFormat(props));
        assertTrue(error.getMessage().contains("Conflicting"));
    }

    @Test
    void shouldFailFastOnUnrecognizedLegacyClass()
    {
        Properties props = new Properties();
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PixelsSinkConfig.resolveKafkaValueFormat(props));
        assertTrue(error.getMessage().contains("Unable to migrate"));
    }
}
