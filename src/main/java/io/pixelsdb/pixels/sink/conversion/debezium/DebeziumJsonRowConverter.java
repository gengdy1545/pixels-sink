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

package io.pixelsdb.pixels.sink.conversion.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;

public final class DebeziumJsonRowConverter
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TableMetadataRegistry tableMetadataRegistry;
    private final DebeziumSourceAdapter configuredAdapter;
    private final DebeziumRowRecordAssembler rowRecordAssembler =
            new DebeziumRowRecordAssembler();

    public DebeziumJsonRowConverter(TableMetadataRegistry tableMetadataRegistry)
    {
        this(tableMetadataRegistry, null);
    }

    public DebeziumJsonRowConverter(
            TableMetadataRegistry tableMetadataRegistry,
            DebeziumSourceAdapter configuredAdapter)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
        this.configuredAdapter = configuredAdapter;
    }

    public RowChangeEvent convert(byte[] data) throws Exception
    {
        JsonNode rootNode = OBJECT_MAPPER.readTree(data);
        JsonNode payloadNode = rootNode.path("payload");
        SinkProto.OperationType operation = DebeziumRecordUtil.getOperationType(
                payloadNode.path("op").asText(""));

        if (!payloadNode.hasNonNull("source"))
        {
            throw new SinkException("Missing source field in row record");
        }

        JsonNode sourceNode = payloadNode.get("source");
        DebeziumSourceAdapter adapter = configuredAdapter == null
                ? DebeziumEnvelopeNormalizer.adapterForSource(sourceNode)
                : configuredAdapter;
        SinkProto.SourceInfo sourceInfo =
                DebeziumEnvelopeNormalizer.normalizeSource(sourceNode, adapter);
        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                sourceInfo.getDb(), sourceInfo.getTable());
        TypeDescription schema = metadata.getTypeDescription();
        DebeziumRowValueConverter rowValueConverter =
                new DebeziumRowValueConverter(schema);

        SinkProto.TransactionInfo transaction = payloadNode.hasNonNull("transaction")
                ? DebeziumEnvelopeNormalizer.normalizeTransaction(
                payloadNode.get("transaction"), adapter)
                : null;
        SinkProto.RowValue before = parseBefore(
                payloadNode, operation, rowValueConverter);
        SinkProto.RowValue after = parseAfter(
                payloadNode, operation, rowValueConverter);
        return rowRecordAssembler.assemble(
                operation, sourceInfo, transaction, before, after, schema, metadata);
    }

    private SinkProto.RowValue parseBefore(
            JsonNode payload,
            SinkProto.OperationType operation,
            DebeziumRowValueConverter converter) throws SinkException
    {
        if (!DebeziumRecordUtil.hasBeforeValue(operation))
        {
            return null;
        }
        JsonNode before = payload.get("before");
        if (before == null || before.isNull())
        {
            throw new SinkException("Missing before image for " + operation);
        }
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        converter.parse(before, builder);
        return builder.build();
    }

    private SinkProto.RowValue parseAfter(
            JsonNode payload,
            SinkProto.OperationType operation,
            DebeziumRowValueConverter converter) throws SinkException
    {
        if (!DebeziumRecordUtil.hasAfterValue(operation))
        {
            return null;
        }
        JsonNode after = payload.get("after");
        if (after == null || after.isNull())
        {
            throw new SinkException("Missing after image for " + operation);
        }
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        converter.parse(after, builder);
        return builder.build();
    }
}
