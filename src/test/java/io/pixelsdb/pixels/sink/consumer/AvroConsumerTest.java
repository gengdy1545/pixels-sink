/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.consumer;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventAvroDeserializer;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class AvroConsumerTest
{
    private static final int MAX_POLL_CYCLES = 100;

    private static KafkaConsumer<String, RowChangeEvent> getRowChangeEventAvroKafkaConsumer()
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RowChangeEventAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SerdeConfig.REGISTRY_URL, registryUrl());
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        props.put(SerdeConfig.CHECK_PERIOD_MS, "30000");

        KafkaConsumer<String, RowChangeEvent> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static void processRecord(RowChangeEvent event)
    {
//        RetinaProto.RowValue.Builder builder = RetinaProto.RowValue.newBuilder();
//        for (SinkProto.ColumnValue value : event.getRowRecord().getAfter().getValuesList()) {
//            builder.addValues(value.getValue());
//        }
//        builder.build();
    }

    private static KafkaConsumer<String, GenericRecord> getStringGenericRecordKafkaConsumer()
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SerdeConfig.REGISTRY_URL, registryUrl());
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        props.put(SerdeConfig.CHECK_PERIOD_MS, "30000");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static RowChangeEvent convertToRowChangeEvent(GenericRecord record, Schema schema) throws SinkException
    {
        return new RowChangeEvent(SinkProto.RowRecord.newBuilder().build(), null);
    }

    private static void processRecord(ConsumerRecord<String, GenericRecord> record, RegistryClient registryClient)
    {
        try
        {
            GenericRecord avroRecord = record.value();
            Schema schema = avroRecord.getSchema();

            String schemaId = getSchemaIdFromRegistry(registryClient, schema);
            System.out.println("Schema ID: " + schemaId);

            RowChangeEvent event = convertToRowChangeEvent(avroRecord, schema);

            System.out.println("Successfully processed message:");
            System.out.println("Topic: " + record.topic());
            System.out.println("Partition: " + record.partition());
            System.out.println("Offset: " + record.offset());
            System.out.println("Event: " + event);

        } catch (Exception e)
        {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String getSchemaIdFromRegistry(RegistryClient client, Schema schema)
    {
        String schemaContent = schema.toString();
        try
        {
            return "";
        } catch (Exception e)
        {
            throw new RuntimeException("Schema not found in registry: " + schema.getFullName(), e);
        }
    }

    @Test
    void shouldConsumeGenericAvroRecords()
    {
        KafkaConsumer<String, GenericRecord> consumer = getStringGenericRecordKafkaConsumer();
        consumer.subscribe(Collections.singletonList(topic()));

        RegistryClient registryClient = RegistryClientFactory.create(registryUrl());

        try
        {
            int recordCount = 0;
            for (int i = 0; i < MAX_POLL_CYCLES; ++i)
            {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records)
                {
                    processRecord(record, registryClient);
                    recordCount++;
                }
            }
            assertTrue(recordCount > 0, "No Avro records were consumed");
        } finally
        {
            consumer.close();
        }
    }

    @Test
    void shouldConsumeRowChangeEvents() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
        KafkaConsumer<String, RowChangeEvent> consumer = getRowChangeEventAvroKafkaConsumer();
        consumer.subscribe(Collections.singletonList(topic()));

        try
        {
            int recordCount = 0;
            for (int i = 0; i < MAX_POLL_CYCLES; ++i)
            {
                ConsumerRecords<String, RowChangeEvent> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, RowChangeEvent> record : records)
                {
                    processRecord(record.value());
                    recordCount++;
                }
            }
            assertTrue(recordCount > 0, "No row-change records were consumed");
        } finally
        {
            consumer.close();
        }
    }

    private static String topic()
    {
        return requiredProperty("pixels.sink.test.topic");
    }

    private static String registryUrl()
    {
        return requiredProperty("pixels.sink.test.registry.url");
    }

    private static String bootstrapServers()
    {
        return requiredProperty("pixels.sink.test.bootstrap.servers");
    }

    private static String groupId()
    {
        return System.getProperty(
                "pixels.sink.test.group.id", "pixels-sink-test-" + UUID.randomUUID());
    }

    private static String requiredProperty(String key)
    {
        String value = System.getProperty(key);
        Assumptions.assumeTrue(value != null && !value.isBlank(),
                "Set -D" + key + " to run this integration test");
        return value;
    }
}