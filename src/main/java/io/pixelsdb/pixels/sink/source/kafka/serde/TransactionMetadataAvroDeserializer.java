/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the license, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.source.kafka.serde;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumAvroTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapterRegistry;
import io.pixelsdb.pixels.sink.source.engine.adapter.DebeziumSourceAdapterSelector;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

public class TransactionMetadataAvroDeserializer
        implements Deserializer<SinkProto.TransactionMetadata>
{
    private final AvroKafkaDeserializer<GenericRecord> avroDeserializer =
            new AvroKafkaDeserializer<>();
    private DebeziumAvroTransactionConverter converter =
            new DebeziumAvroTransactionConverter(
                    DebeziumSourceAdapterSelector.configuredIfPresent());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        Map<String, Object> enrichedConfig = new HashMap<>(configs);
        enrichedConfig.put(SerdeConfig.CHECK_PERIOD_MS, SerdeConfig.CHECK_PERIOD_MS_DEFAULT);
        avroDeserializer.configure(enrichedConfig, isKey);
        Object connector = configs.get(PixelsSinkConstants.DEBEZIUM_CONNECTOR_CLASS);
        if (connector != null && !connector.toString().isBlank())
        {
            DebeziumSourceAdapter adapter =
                    DebeziumSourceAdapterRegistry.resolve(connector.toString());
            converter = new DebeziumAvroTransactionConverter(adapter);
        }
    }

    @Override
    public SinkProto.TransactionMetadata deserialize(String topic, byte[] data)
    {
        if (data == null || data.length == 0)
        {
            return null;
        }
        try
        {
            return converter.convert(avroDeserializer.deserialize(topic, data));
        } catch (Exception e)
        {
            throw new SerializationException(
                    "Failed to deserialize Avro transaction message", e);
        }
    }
}
