/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.conversion.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaRecordConverterTest
{
    @Test
    void shouldCreateConfiguredDeserializerAndConvertRawBytes()
    {
        String converterKey = "test.converter";
        Properties properties = new Properties();
        properties.put(converterKey, Utf8Deserializer.class.getName());

        try (KafkaRecordConverter<String> converter =
                     KafkaRecordConverter.create(properties, converterKey))
        {
            assertEquals("record", converter.convert(
                    "topic", "record".getBytes(StandardCharsets.UTF_8)));
        }
    }

    public static class Utf8Deserializer implements Deserializer<String>
    {
        @Override
        public String deserialize(String topic, byte[] data)
        {
            return new String(data, StandardCharsets.UTF_8);
        }
    }
}
