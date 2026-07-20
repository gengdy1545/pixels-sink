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

import java.util.List;
import java.util.ServiceLoader;

public final class DebeziumSourceAdapterRegistry
{
    private static final List<DebeziumSourceAdapter> ADAPTERS = ServiceLoader
            .load(DebeziumSourceAdapter.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList();

    private DebeziumSourceAdapterRegistry()
    {
    }

    public static DebeziumSourceAdapter forSource(String connector)
    {
        if (connector != null && !connector.isBlank())
        {
            return resolve(connector);
        }
        return configured();
    }

    public static DebeziumSourceAdapter resolve(String connector)
    {
        if (connector == null || connector.isBlank())
        {
            throw new IllegalArgumentException("Unsupported Debezium connector: " + connector);
        }

        List<DebeziumSourceAdapter> matches = ADAPTERS.stream()
                .filter(adapter -> adapter.supports(connector))
                .toList();
        if (matches.size() == 1)
        {
            return matches.get(0);
        }
        if (matches.size() > 1)
        {
            throw new IllegalStateException("Multiple Debezium connector adapters support: " + connector);
        }
        throw new IllegalArgumentException("Unsupported Debezium connector: " + connector);
    }
}
