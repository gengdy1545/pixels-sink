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

package io.pixelsdb.pixels.sink.config.factory;

import io.apicurio.registry.serde.SerdeConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Properties;

import static io.pixelsdb.pixels.sink.config.factory.RowRecordKafkaPropFactory.getCommonKafkaProperties;

public class TransactionKafkaPropFactory implements KafkaPropFactory
{
    @Override
    public Properties createKafkaProperties(PixelsSinkConfig config)
    {
        Properties kafkaProperties = getCommonKafkaProperties(config);
        if (config.getDebeziumConnectorClass() != null &&
                !config.getDebeziumConnectorClass().isBlank())
        {
            kafkaProperties.put(
                    PixelsSinkConstants.DEBEZIUM_CONNECTOR_CLASS,
                    config.getDebeziumConnectorClass());
        }
        if (config.getRegistryUrl() != null && !config.getRegistryUrl().isBlank())
        {
            kafkaProperties.put(SerdeConfig.REGISTRY_URL, config.getRegistryUrl());
        }
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getTransactionTopicGroupId() + "-" + config.getGroupId());
        return kafkaProperties;
    }
}
