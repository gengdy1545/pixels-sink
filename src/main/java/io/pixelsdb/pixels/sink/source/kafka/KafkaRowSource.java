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

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter;
import io.pixelsdb.pixels.sink.util.DataTransform;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaRowSource implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRowSource.class);

    private final Properties kafkaProperties;
    private final String topic;
    private final String tableName;
    private final TablePipelineManager tablePipelineManager;
    private final KafkaRecordConverter<RowChangeEvent> converter;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private KafkaConsumer<String, byte[]> consumer;

    public KafkaRowSource(
            Properties kafkaProperties,
            String topic,
            TablePipelineManager tablePipelineManager)
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.kafkaProperties = new Properties();
        this.kafkaProperties.putAll(kafkaProperties);
        this.kafkaProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId() + "-" + topic);
        this.kafkaProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        this.kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        this.topic = topic;
        this.tableName = DataTransform.extractTableName(topic);
        this.tablePipelineManager = tablePipelineManager;
        this.converter = KafkaRecordConverter.create(
                this.kafkaProperties, PixelsSinkConstants.ROW_RECORD_CONVERTER_CLASS);
    }

    @Override
    public void run()
    {
        try
        {
            consumer = new KafkaConsumer<>(kafkaProperties);
            consumer.subscribe(Collections.singleton(topic));
            while (running.get())
            {
                try
                {
                    ConsumerRecords<String, byte[]> records =
                            consumer.poll(Duration.ofSeconds(5));
                    records.forEach(record ->
                    {
                        byte[] value = record.value();
                        if (value == null)
                        {
                            return;
                        }
                        metricsFacade.addRawData(value.length);
                        try
                        {
                            RowChangeEvent event = converter.convert(topic, value);
                            if (event != null)
                            {
                                metricsFacade.recordSerdRowChange();
                                tablePipelineManager.route(event);
                            }
                        } catch (RuntimeException e)
                        {
                            LOGGER.warn(
                                    "Failed to convert Kafka record from topic {}: {}",
                                    topic, e.getMessage());
                        }
                    });
                } catch (InterruptException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (WakeupException e)
        {
            LOGGER.debug("Consumer wakeup triggered for {}", tableName);
        } catch (Exception e)
        {
            LOGGER.error("Kafka source failed for {}", tableName, e);
        } finally
        {
            converter.close();
            if (consumer != null)
            {
                consumer.close(Duration.ofSeconds(5));
            }
        }
    }

    void requestStop()
    {
        running.set(false);
        if (consumer != null)
        {
            consumer.wakeup();
        }
    }
}
