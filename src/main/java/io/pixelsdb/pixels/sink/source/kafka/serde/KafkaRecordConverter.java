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

package io.pixelsdb.pixels.sink.source.kafka.serde;

import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class KafkaRecordConverter<T> implements AutoCloseable
{
    private final Deserializer<T> deserializer;

    private KafkaRecordConverter(Deserializer<T> deserializer)
    {
        this.deserializer = deserializer;
    }

    public static <T> KafkaRecordConverter<T> create(
            Properties properties, String converterClassKey)
    {
        Object configuredClass = properties.get(converterClassKey);
        if (configuredClass == null)
        {
            throw new IllegalArgumentException(
                    "Missing Kafka record converter: " + converterClassKey);
        }

        try
        {
            Class<?> converterClass = configuredClass instanceof Class<?> type
                    ? type
                    : Class.forName(configuredClass.toString());
            if (!Deserializer.class.isAssignableFrom(converterClass))
            {
                throw new IllegalArgumentException(
                        "Kafka record converter must implement Deserializer: "
                                + converterClass.getName());
            }

            @SuppressWarnings("unchecked")
            Deserializer<T> deserializer =
                    (Deserializer<T>) converterClass.getDeclaredConstructor().newInstance();
            Map<String, Object> configuration = new HashMap<>();
            properties.forEach((key, value) -> configuration.put(key.toString(), value));
            deserializer.configure(configuration, false);
            return new KafkaRecordConverter<>(deserializer);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
                 IllegalAccessException | InvocationTargetException e)
        {
            throw new IllegalArgumentException(
                    "Failed to create Kafka record converter: " + configuredClass, e);
        }
    }

    public T convert(String topic, byte[] data)
    {
        return deserializer.deserialize(topic, data);
    }

    @Override
    public void close()
    {
        deserializer.close();
    }
}
