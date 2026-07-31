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

package io.pixelsdb.pixels.sink.source.kafka.serde;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaRecordConverterTest
{
    @Test
    void shouldCreateJsonRowAndTransactionConverters()
    {
        Properties properties = jsonProperties();
        try (KafkaRecordConverter<RowChangeEvent> row = KafkaRecordConverter.forRow(properties);
             KafkaRecordConverter<SinkProto.TransactionMetadata> tx =
                     KafkaRecordConverter.forTransaction(properties))
        {
            assertNotNull(row);
            assertNotNull(tx);
        }
    }

    @Test
    void shouldReturnNullForEmptyOrNullRowPayload()
    {
        try (KafkaRecordConverter<RowChangeEvent> converter =
                     KafkaRecordConverter.forRow(jsonProperties()))
        {
            assertNull(converter.convert("topic", null));
            assertNull(converter.convert("topic", new byte[0]));
        }
    }

    @Test
    void shouldSwallowInvalidRowPayload()
    {
        try (KafkaRecordConverter<RowChangeEvent> converter =
                     KafkaRecordConverter.forRow(jsonProperties()))
        {
            assertNull(converter.convert("topic", "not-json".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void shouldPropagateInvalidTransactionPayload()
    {
        try (KafkaRecordConverter<SinkProto.TransactionMetadata> converter =
                     KafkaRecordConverter.forTransaction(jsonProperties()))
        {
            assertThrows(RuntimeException.class,
                    () -> converter.convert("topic", "not-json".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private static Properties jsonProperties()
    {
        Properties properties = new Properties();
        properties.put(PixelsSinkConstants.KAFKA_VALUE_FORMAT, "json");
        return properties;
    }
}
