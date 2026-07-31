/*
 * Copyright 2026 PixelsDB.
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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.conversion.debezium.avro;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Decodes Kafka Avro Confluent/Apicurio payloads to {@link GenericRecord}.
 */
public final class DebeziumAvroPayloadDecoder implements AutoCloseable
{
    private final AvroKafkaDeserializer<GenericRecord> avroDeserializer =
            new AvroKafkaDeserializer<>();

    public void configure(Map<String, ?> configs, boolean isKey)
    {
        Map<String, Object> enrichedConfig = new HashMap<>(configs);
        enrichedConfig.put(SerdeConfig.CHECK_PERIOD_MS, SerdeConfig.CHECK_PERIOD_MS_DEFAULT);
        avroDeserializer.configure(enrichedConfig, isKey);
    }

    public GenericRecord decode(String topic, byte[] data)
    {
        return avroDeserializer.deserialize(topic, data);
    }

    @Override
    public void close()
    {
        avroDeserializer.close();
    }
}
