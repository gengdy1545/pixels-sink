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

package io.pixelsdb.pixels.sink.conversion.sinkproto;

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.sink.SinkProto;

import java.nio.ByteBuffer;

public final class TransactionMetadataConverter
{
    public SinkProto.TransactionMetadata convert(ByteBuffer buffer, int loopId)
            throws InvalidProtocolBufferException
    {
        SinkProto.TransactionMetadata metadata =
                SinkProto.TransactionMetadata.parseFrom(buffer.duplicate());
        return metadata.toBuilder()
                .setId(metadata.getId() + "_" + loopId)
                .build();
    }
}
