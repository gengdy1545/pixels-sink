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

package io.pixelsdb.pixels.sink.source.engine;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;

/**
 * Engine-side classification of Debezium transport records.
 * Lives in {@code source.engine}, not conversion.
 */
public interface DebeziumEventClassifier<I>
{
    DebeziumRecordType classify(I input, String transactionTopic);

    SchemaTableName tableOf(I input);
}
