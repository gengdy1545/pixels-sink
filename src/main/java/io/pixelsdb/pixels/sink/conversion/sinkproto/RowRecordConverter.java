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
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.RowChangeEventFactory;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;

import java.nio.ByteBuffer;

public final class RowRecordConverter
{
    private final TableMetadataRegistry tableMetadataRegistry;

    public RowRecordConverter(TableMetadataRegistry tableMetadataRegistry)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
    }

    public RowChangeEvent convert(SinkProto.RowRecord rowRecord) throws SinkException
    {
        SinkProto.SourceInfo source = rowRecord.getSource();
        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                source.getDb(), source.getTable());
        TypeDescription schema = metadata.getTypeDescription();
        return RowChangeEventFactory.create(rowRecord, schema, metadata);
    }

    public SinkProto.RowRecord parse(ByteBuffer buffer) throws InvalidProtocolBufferException
    {
        return SinkProto.RowRecord.parseFrom(buffer.duplicate());
    }
}
