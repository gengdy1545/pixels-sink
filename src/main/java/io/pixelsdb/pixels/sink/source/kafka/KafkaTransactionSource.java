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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.source.kafka;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.pipeline.TransactionPipeline;
import io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaTransactionSource implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTransactionSource.class);

    private final String transactionTopic;
    private final KafkaConsumer<String, byte[]> consumer;
    private final KafkaRecordConverter<SinkProto.TransactionMetadata> converter;
    private final TransactionPipeline transactionPipeline;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public KafkaTransactionSource(
            Properties kafkaProperties,
            String transactionTopic,
            TransactionPipeline transactionPipeline)
    {
        this.transactionTopic = transactionTopic;
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(kafkaProperties);
        this.converter = KafkaRecordConverter.forTransaction(consumerProperties);
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.transactionPipeline = transactionPipeline;
    }

    @Override
    public void run()
    {
        try
        {
            consumer.subscribe(Collections.singletonList(transactionTopic));
            while (running.get())
            {
                try
                {
                    ConsumerRecords<String, byte[]> records =
                            consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, byte[]> record : records)
                    {
                        byte[] value = record.value();
                        if (value == null)
                        {
                            continue;
                        }
                        metricsFacade.addRawData(value.length);
                        SinkProto.TransactionMetadata transaction =
                                converter.convert(transactionTopic, value);
                        if (transaction != null)
                        {
                            metricsFacade.recordSerdTxChange();
                            transactionPipeline.publish(transaction);
                        }
                    }
                } catch (WakeupException e)
                {
                    if (running.get())
                    {
                        throw e;
                    }
                }
            }
        } catch (Exception e)
        {
            if (running.get())
            {
                LOGGER.error("Kafka transaction source failed for {}", transactionTopic, e);
            }
        } finally
        {
            converter.close();
            consumer.close(Duration.ofSeconds(5));
        }
    }

    void requestStop()
    {
        running.set(false);
        consumer.wakeup();
    }
}
