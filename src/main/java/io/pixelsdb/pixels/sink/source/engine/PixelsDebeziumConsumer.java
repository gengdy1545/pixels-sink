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

package io.pixelsdb.pixels.sink.source.engine;


import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumRecordUtil;
import io.pixelsdb.pixels.sink.conversion.debezium.source.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.pipeline.TransactionPipeline;
import io.pixelsdb.pixels.sink.source.engine.adapter.DebeziumStructAdapter;
import io.pixelsdb.pixels.sink.source.engine.adapter.DebeziumSourceAdapterSelector;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;

/**
 * @package: io.pixelsdb.pixels.source
 * @className: PixelsDebeziumConsumer
 * @author: AntiO2
 * @date: 2025/9/25 12:51
 */
public class PixelsDebeziumConsumer
        implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsDebeziumConsumer.class);

    public enum RecordType
    {
        ROW,
        TRANSACTION,
        TOMBSTONE,
        UNKNOWN_CONTROL
    }

    private final String checkTransactionTopic;
    private final DebeziumSourceAdapter connectorAdapter;
    private final DebeziumStructAdapter structAdapter;
    private final TransactionPipeline transactionPipeline = new TransactionPipeline();
    private final TablePipelineManager tablePipelineManager = new TablePipelineManager();
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();

    public PixelsDebeziumConsumer()
    {
        this.checkTransactionTopic = pixelsSinkConfig.getDebeziumTopicPrefix() + ".transaction";
        this.connectorAdapter = DebeziumSourceAdapterSelector.configured();
        this.structAdapter = new DebeziumStructAdapter(connectorAdapter);
    }

    public void start()
    {
        transactionPipeline.start();
    }


    public void handleBatch(List<RecordChangeEvent<SourceRecord>> event,
                            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException
    {
        for (RecordChangeEvent<SourceRecord> record : event)
        {
            try
            {
                SourceRecord sourceRecord = record.record();
                if (sourceRecord == null)
                {
                    continue;
                }

                metricsFacade.recordDebeziumEvent();
                RecordType recordType = classify(sourceRecord, checkTransactionTopic);
                logSourceRecord(sourceRecord, recordType);
                try
                {
                    switch (recordType)
                    {
                        case ROW -> handleRowChangeSourceRecord(sourceRecord);
                        case TRANSACTION -> handleTransactionSourceRecord(sourceRecord);
                        case TOMBSTONE, UNKNOWN_CONTROL ->
                                LOGGER.debug("Skipping Debezium {} event from topic {}",
                                        recordType, sourceRecord.topic());
                    }
                } catch (RuntimeException e)
                {
                    LOGGER.warn("Skipping invalid Debezium {} event from topic {}: {}",
                            recordType, sourceRecord.topic(), e.getMessage());
                }
            } finally
            {
                committer.markProcessed(record);
            }

        }
        committer.markBatchFinished();
    }

    private void handleTransactionSourceRecord(SourceRecord sourceRecord) throws InterruptedException
    {
        SinkProto.TransactionMetadata transaction = structAdapter.toTransactionMetadata(sourceRecord);
        metricsFacade.recordSerdTxChange();
        transactionPipeline.publish(transaction);
    }

    private void handleRowChangeSourceRecord(SourceRecord sourceRecord)
    {
        try
        {
            RowChangeEvent event = structAdapter.toRowEvent(sourceRecord);
            metricsFacade.recordSerdRowChange();
            tablePipelineManager.route(event);
        } catch (SinkException e)
        {
            throw new IllegalArgumentException("Failed to convert Debezium row event", e);
        }
    }

    public static RecordType classify(SourceRecord sourceRecord, String transactionTopic)
    {
        if (sourceRecord == null || sourceRecord.value() == null)
        {
            return RecordType.TOMBSTONE;
        }
        if (!(sourceRecord.value() instanceof Struct value))
        {
            return RecordType.UNKNOWN_CONTROL;
        }

        if (transactionTopic != null && transactionTopic.equals(sourceRecord.topic()))
        {
            String status = DebeziumRecordUtil.getStringSafely(value, "status");
            String id = DebeziumRecordUtil.getStringSafely(value, "id");
            return (status.equals("BEGIN") || status.equals("END")) && !id.isBlank()
                    ? RecordType.TRANSACTION
                    : RecordType.UNKNOWN_CONTROL;
        }

        return isRowChange(value) ? RecordType.ROW : RecordType.UNKNOWN_CONTROL;
    }

    private static boolean isRowChange(Struct value)
    {
        String op = DebeziumRecordUtil.getStringSafely(value, "op").toLowerCase(Locale.ROOT);
        if (!(op.equals("c") || op.equals("u") || op.equals("d") || op.equals("r")))
        {
            return false;
        }

        Object sourceObject = DebeziumRecordUtil.getFieldSafely(value, "source");
        if (!(sourceObject instanceof Struct source) ||
                DebeziumRecordUtil.getStringSafely(source, "db").isBlank() ||
                DebeziumRecordUtil.getStringSafely(source, "table").isBlank())
        {
            return false;
        }

        Object before = DebeziumRecordUtil.getFieldSafely(value, "before");
        Object after = DebeziumRecordUtil.getFieldSafely(value, "after");
        return switch (op)
        {
            case "c", "r" -> after instanceof Struct;
            case "u" -> before instanceof Struct && after instanceof Struct;
            case "d" -> before instanceof Struct;
            default -> false;
        };
    }

    private void logSourceRecord(SourceRecord record, RecordType recordType)
    {
        if (!LOGGER.isDebugEnabled())
        {
            return;
        }
        Struct value = record.value() instanceof Struct struct ? struct : null;
        Struct source = value == null ? null :
                asStruct(DebeziumRecordUtil.getFieldSafely(value, "source"));
        Struct transaction = value == null ? null :
                asStruct(DebeziumRecordUtil.getFieldSafely(value, "transaction"));
        String rawTransactionId = recordType == RecordType.TRANSACTION
                ? DebeziumRecordUtil.getStringSafely(value, "id")
                : DebeziumRecordUtil.getStringSafely(transaction, "id");
        String canonicalTransactionId =
                connectorAdapter.normalizeTransactionId(rawTransactionId);

        LOGGER.debug("Debezium SourceRecord topic={}, sourcePartition={}, sourceOffset={}, " +
                        "keySchema={}, key={}, valueSchema={}, category={}, transaction.id={}, " +
                        "source.gtid={}, source.file={}, source.pos={}, source.row={}",
                record.topic(), record.sourcePartition(), record.sourceOffset(),
                schemaName(record.keySchema()), record.key(), schemaName(record.valueSchema()),
                recordType, canonicalTransactionId,
                DebeziumRecordUtil.getStringSafely(source, "gtid"),
                DebeziumRecordUtil.getStringSafely(source, "file"),
                DebeziumRecordUtil.getStringSafely(source, "pos"),
                DebeziumRecordUtil.getStringSafely(source, "row"));
    }

    private static Struct asStruct(Object value)
    {
        return value instanceof Struct struct ? struct : null;
    }

    private static String schemaName(org.apache.kafka.connect.data.Schema schema)
    {
        return schema == null ? "" : String.valueOf(schema.name());
    }

    @Override
    public void close()
    {
        tablePipelineManager.close();
        transactionPipeline.close();
    }

    public void abort()
    {
        tablePipelineManager.abort();
        transactionPipeline.abort();
    }
}
