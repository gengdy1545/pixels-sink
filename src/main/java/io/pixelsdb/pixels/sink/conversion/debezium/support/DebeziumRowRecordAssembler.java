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

package io.pixelsdb.pixels.sink.conversion.debezium.support;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.RowChangeEventFactory;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;

public final class DebeziumRowRecordAssembler
{
    public RowChangeEvent assemble(
            SinkProto.OperationType operation,
            SinkProto.SourceInfo source,
            SinkProto.TransactionInfo transaction,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TypeDescription schema) throws SinkException
    {
        return assemble(operation, source, transaction, before, after, schema, null);
    }

    public RowChangeEvent assemble(
            SinkProto.OperationType operation,
            SinkProto.SourceInfo source,
            SinkProto.TransactionInfo transaction,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TypeDescription schema,
            TableMetadata metadata) throws SinkException
    {
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
        return metadata == null
                ? RowChangeEventFactory.create(builder.build(), schema)
                : RowChangeEventFactory.create(builder.build(), schema, metadata);
    }
}
