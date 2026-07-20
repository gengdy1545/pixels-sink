/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.sink.conversion.debezium.source;

import io.pixelsdb.pixels.sink.SinkProto;

public final class MySqlSourceAdapter implements DebeziumSourceAdapter
{
    public static final MySqlSourceAdapter INSTANCE = new MySqlSourceAdapter();

    public MySqlSourceAdapter()
    {
    }

    @Override
    public String connector()
    {
        return "mysql";
    }

    @Override
    public String normalizeTransactionId(String sourceId)
    {
        return sourceId;
    }

    @Override
    public SinkProto.SourceInfo normalizeSource(DebeziumSourceMetadata source)
    {
        return SinkProto.SourceInfo.newBuilder()
                .setDb(source.db())
                .setTable(source.table())
                .build();
    }
}
