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

package io.pixelsdb.pixels.sink.source.storage;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.sinkproto.RowRecordConverter;
import io.pixelsdb.pixels.sink.conversion.sinkproto.TransactionMetadataConverter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.pipeline.TransactionPipeline;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;
import io.pixelsdb.pixels.sink.util.DataTransform;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiter;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractSinkStorageSource implements SinkSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSinkStorageSource.class);
    protected final AtomicBoolean running = new AtomicBoolean(false);

    protected final String topic;
    protected final String baseDir;
    protected final EtcdFileRegistry etcdFileRegistry;
    protected final List<String> files;
    protected final CompletableFuture<ByteBuffer> POISON_PILL = new CompletableFuture<>();
    protected final Map<Integer, Thread> consumerThreads = new ConcurrentHashMap<>();
    protected final Map<Integer, BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>>> queueMap = new ConcurrentHashMap<>();
    protected final boolean storageLoopEnabled;
    protected final FlushRateLimiter sourceRateLimiter;
    protected final TablePipelineManager tablePipelineManager = new TablePipelineManager();
    protected final TransactionPipeline transactionPipeline = new TransactionPipeline();
    protected final RowRecordConverter rowRecordConverter;
    protected final TransactionMetadataConverter transactionMetadataConverter =
            new TransactionMetadataConverter();
    protected final boolean freshnessTimestamp;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    protected int loopId = 0;
    protected List<PhysicalReader> readers = new ArrayList<>();

    protected AbstractSinkStorageSource()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.topic = pixelsSinkConfig.getSinkProtoData();
        this.baseDir = pixelsSinkConfig.getSinkProtoDir();
        this.etcdFileRegistry = new EtcdFileRegistry(topic, baseDir);
        this.files = this.etcdFileRegistry.listAllFiles();
        this.storageLoopEnabled = pixelsSinkConfig.isSinkStorageLoop();
        this.rowRecordConverter = new RowRecordConverter(TableMetadataRegistry.Instance());
        this.freshnessTimestamp = pixelsSinkConfig.isSinkMonitorFreshnessTimestamp();
        this.sourceRateLimiter = FlushRateLimiterFactory.getNewInstance();
    }

    abstract ProtoType getProtoType(int i);

    protected void clean()
    {
        queueMap.values().forEach(q ->
        {
            try
            {
                q.put(new Pair<>(POISON_PILL, loopId));
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        consumerThreads.values().forEach(t ->
        {
            try
            {
                t.join();
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        for (PhysicalReader reader : readers)
        {
            try
            {
                reader.close();
            } catch (IOException e)
            {
                LOGGER.warn("Failed to close reader", e);
            }
        }
        tablePipelineManager.close();
        transactionPipeline.close();
    }

    protected void handleTransactionSourceRecord(ByteBuffer record, Integer loopId)
    {
        try
        {
            SinkProto.TransactionMetadata metadata =
                    transactionMetadataConverter.convert(record, loopId);
            metricsFacade.recordSerdTxChange();
            transactionPipeline.publish(metadata);
        } catch (Exception e)
        {
            LOGGER.warn("Failed to convert storage transaction metadata", e);
        }
    }

    protected void consumeQueue(int key, BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue, ProtoType protoType)
    {
        try
        {
            while (true)
            {
                Pair<CompletableFuture<ByteBuffer>, Integer> pair = queue.take();
                CompletableFuture<ByteBuffer> value = pair.getLeft();
                int loopId = pair.getRight();
                if (value == POISON_PILL)
                {
                    break;
                }
                ByteBuffer valueBuffer = value.get();
                metricsFacade.recordDebeziumEvent();
                switch (protoType)
                {
                    case ROW -> handleRowChangeSourceRecord(key, valueBuffer, loopId);
                    case TRANS -> handleTransactionSourceRecord(valueBuffer, loopId);
                }
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e)
        {
            LOGGER.error("Error in async processing", e);
        }
    }

    protected ByteBuffer copyToHeap(ByteBuffer directBuffer)
    {
        ByteBuffer duplicate = directBuffer.duplicate();
        ByteBuffer heapBuffer = ByteBuffer.allocate(duplicate.remaining());
        heapBuffer.put(duplicate);
        heapBuffer.flip();
        return heapBuffer;
    }

    protected void handleRowChangeSourceRecord(int key, ByteBuffer dataBuffer, int loopId)
    {
        try
        {
            SinkProto.RowRecord.Builder builder =
                    rowRecordConverter.parse(dataBuffer).toBuilder();
            if (freshnessTimestamp)
            {
                DataTransform.updateRecordTimestamp(builder, System.currentTimeMillis() * 1000);
            }
            if (builder.hasTransaction())
            {
                SinkProto.TransactionInfo transaction = builder.getTransaction();
                builder.setTransaction(transaction.toBuilder()
                        .setId(transaction.getId() + "_" + loopId)
                        .build());
            }
            RowChangeEvent event = rowRecordConverter.convert(builder.build());
            metricsFacade.recordSerdRowChange();
            tablePipelineManager.route(event);
        } catch (Exception e)
        {
            LOGGER.warn("Failed to convert storage row record", e);
        }
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }

    @Override
    public void stopProcessor()
    {
        running.set(false);
        tablePipelineManager.close();
        transactionPipeline.close();
    }
}
