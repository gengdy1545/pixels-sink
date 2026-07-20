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

package io.pixelsdb.pixels.sink.conversion.kafka;

import java.util.Properties;

/**
 * @deprecated Kafka source wiring belongs to {@code source.kafka.serde}.
 * This facade keeps the old SPI name usable by existing deployments.
 */
@Deprecated
public final class KafkaRecordConverter<T> implements AutoCloseable
{
    private final io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter<T> delegate;

    private KafkaRecordConverter(
            io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter<T> delegate)
    {
        this.delegate = delegate;
    }

    public static <T> KafkaRecordConverter<T> create(
            Properties properties, String converterClassKey)
    {
        return new KafkaRecordConverter<>(
                io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter.create(
                        properties, converterClassKey));
    }

    public T convert(String topic, byte[] data)
    {
        return delegate.convert(topic, data);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
