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
 * You should have received a copy of the GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Decodes a batch in parallel and returns results in input order.
 * Failed records become {@code null} entries.
 */
public final class OrderedBatchDecoder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderedBatchDecoder.class);

    private OrderedBatchDecoder()
    {
    }

    public static <T, R> List<R> decodeInOrder(
            ExecutorService executor,
            List<T> records,
            Function<T, R> decoder) throws InterruptedException
    {
        List<Future<R>> futures = new ArrayList<>(records.size());
        for (T record : records)
        {
            futures.add(executor.submit(() -> decoder.apply(record)));
        }

        List<R> decodedRecords = new ArrayList<>(records.size());
        for (Future<R> future : futures)
        {
            try
            {
                decodedRecords.add(future.get());
            } catch (ExecutionException e)
            {
                LOGGER.warn("Failed to decode record", e.getCause());
                decodedRecords.add(null);
            }
        }
        return decodedRecords;
    }
}
