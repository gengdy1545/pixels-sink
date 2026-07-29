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

package io.pixelsdb.pixels.sink.conversion.debezium;


import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @package: io.pixelsdb.pixels.sink.conversion.debezium
 * @className: RowChangeEventStructConverter
 * @author: AntiO2
 * @date: 2025/9/26 12:00
 */
public class RowChangeEventStructConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RowChangeEventStructConverter.class);
    private final TableMetadataRegistry tableMetadataRegistry;
    private final DebeziumSourceAdapter configuredAdapter;

    public RowChangeEventStructConverter()
    {
        this(TableMetadataRegistry.Instance(), null);
    }

    public RowChangeEventStructConverter(TableMetadataRegistry tableMetadataRegistry)
    {
        this(tableMetadataRegistry, null);
    }

    public RowChangeEventStructConverter(
            TableMetadataRegistry tableMetadataRegistry,
            DebeziumSourceAdapter configuredAdapter)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
        this.configuredAdapter = configuredAdapter;
    }

    public static RowChangeEvent convertToRowChangeEvent(SourceRecord sourceRecord) throws SinkException
    {
        return new RowChangeEventStructConverter().convert(sourceRecord);
    }

    public RowChangeEvent convert(SourceRecord sourceRecord) throws SinkException
    {
        if (!(sourceRecord.value() instanceof Struct value))
        {
            throw new SinkException("Debezium row value must be a Struct");
        }
        String op = DebeziumRecordUtil.getStringSafely(value, "op");
        SinkProto.OperationType operationType = DebeziumRecordUtil.getOperationType(op);
        return buildRowRecord(value, operationType);
    }

    private RowChangeEvent buildRowRecord(Struct value,
                                          SinkProto.OperationType opType) throws SinkException
    {

        String schemaName;
        String tableName;
        DebeziumSourceAdapter adapter;
        SinkProto.SourceInfo sourceInfo;
        try
        {
            Struct source = value.getStruct("source");
            if (source == null)
            {
                throw new SinkException("Missing source field in row record");
            }
            adapter = configuredAdapter == null
                    ? DebeziumEnvelopeNormalizer.adapterForSource(source)
                    : configuredAdapter;
            sourceInfo = DebeziumEnvelopeNormalizer.normalizeSource(source, adapter);
            schemaName = sourceInfo.getDb();
            tableName = sourceInfo.getTable();
        } catch (DataException | IllegalArgumentException e)
        {
            LOGGER.warn("Missing source field in row record");
            throw new SinkException(e);
        }

        TableMetadata metadata = tableMetadataRegistry.getMetadata(schemaName, tableName);
        TypeDescription typeDescription = metadata.getTypeDescription();
        DebeziumRowValueConverter rowDataParser = new DebeziumRowValueConverter(typeDescription);

        SinkProto.TransactionInfo transactionInfo = null;
        try
        {
            Struct transaction = value.getStruct("transaction");
            if (transaction != null)
            {
                transactionInfo = DebeziumEnvelopeNormalizer.normalizeTransaction(transaction, adapter);
            }
        } catch (DataException e)
        {
            LOGGER.warn("Missing transaction field in row record");
        }

        SinkProto.RowValue beforeValue = null;
        if (DebeziumRecordUtil.hasBeforeValue(opType))
        {
            Struct before = value.getStruct("before");
            if (before == null)
            {
                throw new SinkException("Missing before image for " + opType);
            }
            SinkProto.RowValue.Builder beforeBuilder = SinkProto.RowValue.newBuilder();
            rowDataParser.parse(before, beforeBuilder);
            beforeValue = beforeBuilder.build();
        }

        SinkProto.RowValue afterValue = null;
        if (DebeziumRecordUtil.hasAfterValue(opType))
        {
            Struct after = value.getStruct("after");
            if (after == null)
            {
                throw new SinkException("Missing after image for " + opType);
            }
            SinkProto.RowValue.Builder afterBuilder = SinkProto.RowValue.newBuilder();
            rowDataParser.parse(after, afterBuilder);
            afterValue = afterBuilder.build();
        }

        return new DebeziumRowRecordAssembler().assemble(
                opType, sourceInfo, transactionInfo, beforeValue, afterValue,
                typeDescription, metadata);
    }
}
