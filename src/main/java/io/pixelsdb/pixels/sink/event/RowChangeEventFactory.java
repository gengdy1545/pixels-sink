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

package io.pixelsdb.pixels.sink.event;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;

public final class RowChangeEventFactory
{
    private RowChangeEventFactory()
    {
    }

    public static RowChangeEvent create(
            SinkProto.OperationType operation,
            SinkProto.SourceInfo source,
            SinkProto.TransactionInfo transaction,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TypeDescription schema) throws SinkException
    {
        if (operation == null)
        {
            throw new SinkException("Row change operation is missing");
        }
        if (source == null)
        {
            throw new SinkException("Row change source is missing");
        }

        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder()
                .setOp(operation)
                .setSource(source);
        if (transaction != null)
        {
            builder.setTransaction(transaction);
        }
        if (before != null)
        {
            builder.setBefore(before);
        }
        if (after != null)
        {
            builder.setAfter(after);
        }
        return create(builder.build(), schema);
    }

    public static RowChangeEvent create(
            SinkProto.RowRecord rowRecord,
            TypeDescription schema) throws SinkException
    {
        if (rowRecord == null)
        {
            throw new SinkException("Row change record is missing");
        }
        if (schema == null)
        {
            throw new SinkException("Row change schema is missing");
        }
        TableMetadataRegistry registry = TableMetadataRegistry.Instance();
        TableMetadata metadata = registry.getMetadata(
                rowRecord.getSource().getDb(), rowRecord.getSource().getTable());
        return create(rowRecord, schema, metadata);
    }

    public static RowChangeEvent create(
            SinkProto.RowRecord rowRecord,
            TypeDescription schema,
            TableMetadata metadata) throws SinkException
    {
        if (rowRecord == null)
        {
            throw new SinkException("Row change record is missing");
        }
        if (schema == null)
        {
            throw new SinkException("Row change schema is missing");
        }

        RowChangeEvent event = new RowChangeEvent(rowRecord, schema, metadata);
        event.initIndexKey();
        return event;
    }
}
