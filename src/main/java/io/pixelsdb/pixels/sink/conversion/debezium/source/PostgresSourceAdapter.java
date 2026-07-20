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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PostgresSourceAdapter implements DebeziumSourceAdapter
{
    public static final PostgresSourceAdapter INSTANCE = new PostgresSourceAdapter();
    private static final Pattern TRANSACTION_WITH_LSN = Pattern.compile("^(\\d+):\\d+$");

    public PostgresSourceAdapter()
    {
    }

    @Override
    public String connector()
    {
        return "postgresql";
    }

    @Override
    public String normalizeTransactionId(String sourceId)
    {
        Matcher matcher = TRANSACTION_WITH_LSN.matcher(sourceId);
        return matcher.matches() ? matcher.group(1) : sourceId;
    }

    @Override
    public SinkProto.SourceInfo normalizeSource(DebeziumSourceMetadata source)
    {
        return SinkProto.SourceInfo.newBuilder()
                .setDb(source.db())
                .setSchema(source.schema())
                .setTable(source.table())
                .build();
    }
}
