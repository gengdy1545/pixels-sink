/*
 * Copyright 2026 PixelsDB.
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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.config;

import java.util.Locale;

/**
 * Resolves {@code sink.datasource.engine.format}. Only {@code connect} is implemented.
 */
public final class EngineValueFormat
{
    public static final String CONNECT = "connect";

    private EngineValueFormat()
    {
    }

    public static String resolve(String configuredFormat)
    {
        if (configuredFormat == null || configuredFormat.isBlank())
        {
            return CONNECT;
        }
        String normalized = normalize(configuredFormat);
        if (!CONNECT.equals(normalized))
        {
            throw new IllegalArgumentException(
                    "sink.datasource.engine.format='" + configuredFormat +
                            "' is not implemented yet; only 'connect' is supported. " +
                            "Json/Avro Engine paths will reuse conversion.debezium converters later.");
        }
        return normalized;
    }

    private static String normalize(String format)
    {
        return format.trim().toLowerCase(Locale.ROOT);
    }
}
