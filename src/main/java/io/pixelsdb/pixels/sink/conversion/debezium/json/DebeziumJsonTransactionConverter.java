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

package io.pixelsdb.pixels.sink.conversion.debezium.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumEnvelopeNormalizer;

/**
 * Converts Debezium JSON transaction envelopes ({@code byte[]} payload).
 */
public final class DebeziumJsonTransactionConverter
        implements DebeziumTransactionConverter<byte[]>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DebeziumSourceAdapter configuredAdapter;

    public DebeziumJsonTransactionConverter(DebeziumSourceAdapter configuredAdapter)
    {
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public SinkProto.TransactionMetadata convert(byte[] data) throws Exception
    {
        if (configuredAdapter == null)
        {
            throw new IllegalStateException(
                    "No Debezium source adapter configured for JSON transaction conversion");
        }
        JsonNode payload = OBJECT_MAPPER.readTree(data).path("payload");
        return DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(
                payload, configuredAdapter);
    }
}
