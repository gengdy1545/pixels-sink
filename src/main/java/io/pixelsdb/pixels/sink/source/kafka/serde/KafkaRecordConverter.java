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
 * You should have received a copy of the GNU Affero General Public License
 * along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.source.kafka.serde;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.KafkaValueFormat;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.conversion.debezium.avro.DebeziumAvroPayloadDecoder;
import io.pixelsdb.pixels.sink.conversion.debezium.avro.DebeziumAvroRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.avro.DebeziumAvroTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapterRegistry;
import io.pixelsdb.pixels.sink.conversion.debezium.json.DebeziumJsonRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.json.DebeziumJsonTransactionConverter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Assembles Debezium row/tx converters for Kafka by {@code sink.kafka.value.format}.
 */
public final class KafkaRecordConverter<T> implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordConverter.class);

    private enum ErrorPolicy
    {
        SWALLOW,
        PROPAGATE
    }

    @FunctionalInterface
    private interface ConvertFn<T>
    {
        T convert(String topic, byte[] data) throws Exception;
    }

    private final ConvertFn<T> convertFn;
    private final AutoCloseable resource;
    private final ErrorPolicy errorPolicy;

    private KafkaRecordConverter(
            ConvertFn<T> convertFn, AutoCloseable resource, ErrorPolicy errorPolicy)
    {
        this.convertFn = convertFn;
        this.resource = resource;
        this.errorPolicy = errorPolicy;
    }

    public static KafkaRecordConverter<RowChangeEvent> forRow(Properties properties)
    {
        String format = resolveFormat(properties);
        DebeziumSourceAdapter adapter = resolveAdapter(properties);
        return switch (format)
        {
            case KafkaValueFormat.JSON ->
            {
                DebeziumJsonRowConverter converter = new DebeziumJsonRowConverter(
                        TableMetadataRegistry.Instance(), adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(data), null, ErrorPolicy.SWALLOW);
            }
            case KafkaValueFormat.AVRO ->
            {
                DebeziumAvroPayloadDecoder decoder = new DebeziumAvroPayloadDecoder();
                decoder.configure(toConfigMap(properties), false);
                DebeziumAvroRowConverter converter = new DebeziumAvroRowConverter(
                        TableMetadataRegistry.Instance(), adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(decoder.decode(topic, data)),
                        decoder,
                        ErrorPolicy.SWALLOW);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Kafka value format: " + format);
        };
    }

    public static KafkaRecordConverter<SinkProto.TransactionMetadata> forTransaction(
            Properties properties)
    {
        String format = resolveFormat(properties);
        DebeziumSourceAdapter adapter = resolveAdapter(properties);
        return switch (format)
        {
            case KafkaValueFormat.JSON ->
            {
                DebeziumJsonTransactionConverter converter =
                        new DebeziumJsonTransactionConverter(adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(data), null, ErrorPolicy.PROPAGATE);
            }
            case KafkaValueFormat.AVRO ->
            {
                DebeziumAvroPayloadDecoder decoder = new DebeziumAvroPayloadDecoder();
                decoder.configure(toConfigMap(properties), false);
                DebeziumAvroTransactionConverter converter =
                        new DebeziumAvroTransactionConverter(adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(decoder.decode(topic, data)),
                        decoder,
                        ErrorPolicy.PROPAGATE);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Kafka value format: " + format);
        };
    }

    public T convert(String topic, byte[] data)
    {
        if (data == null || data.length == 0)
        {
            return null;
        }
        try
        {
            return convertFn.convert(topic, data);
        } catch (RuntimeException e)
        {
            if (errorPolicy == ErrorPolicy.SWALLOW)
            {
                LOGGER.warn("Failed to convert Kafka record from topic {}", topic, e);
                return null;
            }
            throw e;
        } catch (Exception e)
        {
            if (errorPolicy == ErrorPolicy.SWALLOW)
            {
                LOGGER.warn("Failed to convert Kafka record from topic {}", topic, e);
                return null;
            }
            throw new RuntimeException("Failed to convert Kafka record from " + topic, e);
        }
    }

    @Override
    public void close()
    {
        if (resource != null)
        {
            try
            {
                resource.close();
            } catch (Exception e)
            {
                LOGGER.warn("Failed to close Kafka record converter resource", e);
            }
        }
    }

    private static String resolveFormat(Properties properties)
    {
        Object configured = properties.get(PixelsSinkConstants.KAFKA_VALUE_FORMAT);
        return KafkaValueFormat.resolve(configured == null ? null : configured.toString());
    }

    private static DebeziumSourceAdapter resolveAdapter(Properties properties)
    {
        Object connector = properties.get(PixelsSinkConstants.DEBEZIUM_CONNECTOR_CLASS);
        if (connector == null || connector.toString().isBlank())
        {
            return null;
        }
        return DebeziumSourceAdapterRegistry.resolve(connector.toString());
    }

    private static Map<String, Object> toConfigMap(Properties properties)
    {
        Map<String, Object> configuration = new HashMap<>();
        properties.forEach((key, value) -> configuration.put(key.toString(), value));
        return configuration;
    }
}
