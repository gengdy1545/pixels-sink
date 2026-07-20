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

package io.pixelsdb.pixels.sink.source.engine.adapter;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumEnvelopeNormalizer;
import io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventStructConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public final class DebeziumStructAdapter
{
    private final DebeziumSourceAdapter sourceAdapter;
    private final RowChangeEventStructConverter rowConverter;

    public DebeziumStructAdapter(DebeziumSourceAdapter sourceAdapter)
    {
        this.sourceAdapter = sourceAdapter;
        this.rowConverter = new RowChangeEventStructConverter(
                TableMetadataRegistry.Instance(), sourceAdapter);
    }

    public RowChangeEvent toRowEvent(SourceRecord sourceRecord) throws SinkException
    {
        return rowConverter.convert(sourceRecord);
    }

    public SinkProto.TransactionMetadata toTransactionMetadata(SourceRecord sourceRecord)
    {
        if (!(sourceRecord.value() instanceof Struct value))
        {
            throw new IllegalArgumentException("Debezium transaction value must be a Struct");
        }
        return DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(value, sourceAdapter);
    }
}
