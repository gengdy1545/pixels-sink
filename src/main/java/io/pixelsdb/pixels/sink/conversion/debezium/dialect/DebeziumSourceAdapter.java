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
package io.pixelsdb.pixels.sink.conversion.debezium.dialect;

import io.pixelsdb.pixels.sink.SinkProto;

import java.util.Locale;

public interface DebeziumSourceAdapter
{
    String connector();

    default boolean supports(String connectorName)
    {
        if (connectorName == null || connectorName.isBlank())
        {
            return false;
        }
        String normalized = connectorName.toLowerCase(Locale.ROOT);
        return normalized.equals(connector()) || normalized.contains(connector());
    }

    String normalizeTransactionId(String sourceId);

    SinkProto.SourceInfo normalizeSource(DebeziumSourceMetadata source);

    default String normalizeDataCollection(String sourceDataCollection)
    {
        return sourceDataCollection;
    }
}
