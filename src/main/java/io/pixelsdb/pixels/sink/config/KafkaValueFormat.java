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
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.config;

import java.util.Locale;

/**
 * Resolves {@code sink.kafka.value.format}. Supported values: {@code json}, {@code avro}.
 */
public final class KafkaValueFormat
{
    public static final String JSON = "json";
    public static final String AVRO = "avro";

    private KafkaValueFormat()
    {
    }

    public static String resolve(String configuredFormat)
    {
        if (configuredFormat == null || configuredFormat.isBlank())
        {
            return JSON;
        }
        return normalize(configuredFormat);
    }

    private static String normalize(String format)
    {
        String normalized = format.trim().toLowerCase(Locale.ROOT);
        if (!JSON.equals(normalized) && !AVRO.equals(normalized))
        {
            throw new IllegalArgumentException(
                    "sink.kafka.value.format must be json or avro, got: " + format);
        }
        return normalized;
    }
}
