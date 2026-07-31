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

package io.pixelsdb.pixels.sink.conversion.debezium.avro;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumEnvelopeNormalizer;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRecordUtil;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRowRecordAssembler;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRowValueConverter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.avro.generic.GenericRecord;

public final class DebeziumAvroRowConverter implements DebeziumRowConverter<GenericRecord>
{
    private final TableMetadataRegistry tableMetadataRegistry;
    private final DebeziumSourceAdapter configuredAdapter;
    private final DebeziumRowRecordAssembler rowRecordAssembler =
            new DebeziumRowRecordAssembler();

    public DebeziumAvroRowConverter(
            TableMetadataRegistry tableMetadataRegistry,
            DebeziumSourceAdapter configuredAdapter)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public RowChangeEvent convert(GenericRecord record) throws SinkException
    {
        SinkProto.OperationType operation = DebeziumRecordUtil.getOperationType(
                DebeziumRecordUtil.getStringSafely(record, "op"));
        Object sourceObject = record.get("source");
        if (!(sourceObject instanceof GenericRecord source))
        {
            throw new SinkException("Missing source field in row record");
        }
        DebeziumSourceAdapter adapter = configuredAdapter == null
                ? DebeziumEnvelopeNormalizer.adapterForSource(source)
                : configuredAdapter;
        SinkProto.SourceInfo sourceInfo =
                DebeziumEnvelopeNormalizer.normalizeSource(source, adapter);
        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                sourceInfo.getDb(), sourceInfo.getTable());
        TypeDescription schema = metadata.getTypeDescription();
        DebeziumRowValueConverter rowValueConverter =
                new DebeziumRowValueConverter(schema);

        SinkProto.RowValue before = parseRowValue(
                record.get("before"), operation, true, rowValueConverter);
        SinkProto.RowValue after = parseRowValue(
                record.get("after"), operation, false, rowValueConverter);
        SinkProto.TransactionInfo transaction = record.get("transaction") == null
                ? null
                : DebeziumEnvelopeNormalizer.normalizeTransaction(
                record.get("transaction"), adapter);
        return rowRecordAssembler.assemble(
                operation, sourceInfo, transaction, before, after, schema, metadata);
    }

    private SinkProto.RowValue parseRowValue(
            Object value,
            SinkProto.OperationType operation,
            boolean before,
            DebeziumRowValueConverter converter) throws SinkException
    {
        boolean required = before
                ? DebeziumRecordUtil.hasBeforeValue(operation)
                : DebeziumRecordUtil.hasAfterValue(operation);
        if (!required)
        {
            return null;
        }
        if (!(value instanceof GenericRecord row))
        {
            throw new SinkException(
                    "Missing " + (before ? "before" : "after") + " image for " + operation);
        }
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        converter.parse(row, builder);
        return builder.build();
    }
}
