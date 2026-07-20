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

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public final class TransactionMetadataDeserializer
        implements Deserializer<SinkProto.TransactionMetadata>
{
    @Override
    public SinkProto.TransactionMetadata deserialize(String topic, byte[] data)
    {
        if (data == null || data.length == 0)
        {
            return null;
        }
        try
        {
            return SinkProto.TransactionMetadata.parseFrom(data);
        } catch (InvalidProtocolBufferException e)
        {
            throw new SerializationException(
                    "Failed to deserialize canonical transaction metadata from " + topic, e);
        }
    }
}
