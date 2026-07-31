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

package io.pixelsdb.pixels.sink.conversion.debezium.connect;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumEnvelopeNormalizer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Converts Kafka Connect {@link SourceRecord} transaction envelopes.
 */
public final class DebeziumConnectTransactionConverter
        implements DebeziumTransactionConverter<SourceRecord>
{
    private final DebeziumSourceAdapter configuredAdapter;

    public DebeziumConnectTransactionConverter(DebeziumSourceAdapter configuredAdapter)
    {
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public SinkProto.TransactionMetadata convert(SourceRecord sourceRecord)
    {
        if (!(sourceRecord.value() instanceof Struct value))
        {
            throw new IllegalArgumentException("Debezium transaction value must be a Struct");
        }
        return configuredAdapter == null
                ? DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(value)
                : DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(value, configuredAdapter);
    }
}
