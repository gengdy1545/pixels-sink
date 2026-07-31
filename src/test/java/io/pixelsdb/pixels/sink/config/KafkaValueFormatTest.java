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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaValueFormatTest
{
    @Test
    void shouldDefaultToJson()
    {
        assertEquals("json", KafkaValueFormat.resolve(null));
        assertEquals("json", KafkaValueFormat.resolve(""));
        assertEquals("json", KafkaValueFormat.resolve("  "));
    }

    @Test
    void shouldNormalizeConfiguredFormat()
    {
        assertEquals("json", KafkaValueFormat.resolve("JSON"));
        assertEquals("avro", KafkaValueFormat.resolve("Avro"));
    }

    @Test
    void shouldRejectUnknownFormat()
    {
        assertThrows(IllegalArgumentException.class,
                () -> KafkaValueFormat.resolve("protobuf"));
    }
}
