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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumEnvelopeNormalizer;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapterRegistry;
import io.pixelsdb.pixels.sink.source.engine.adapter.DebeziumSourceAdapterSelector;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransactionMetadataJsonDeserializer
        implements Deserializer<SinkProto.TransactionMetadata>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TransactionMetadataJsonDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private DebeziumSourceAdapter adapter =
            DebeziumSourceAdapterSelector.configuredIfPresent();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        Object connector = configs.get(PixelsSinkConstants.DEBEZIUM_CONNECTOR_CLASS);
        if (connector != null && !connector.toString().isBlank())
        {
            adapter = DebeziumSourceAdapterRegistry.resolve(connector.toString());
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
            JsonNode payload = OBJECT_MAPPER.readTree(data).path("payload");
            if (adapter == null)
            {
                throw new IllegalStateException(
                        "No Debezium source adapter configured for " + topic);
            }
            return DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(payload, adapter);
        } catch (Exception e)
        {
            LOGGER.error("Failed to deserialize transaction message from {}", topic, e);
            throw new RuntimeException("Deserialization error", e);
        }
    }
}
