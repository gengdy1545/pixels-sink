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

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import org.slf4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared fixed-size decode pools with caller-runs backpressure.
 * Tasks submitted after shutdown are rejected.
 */
public final class DecodeExecutors
{
    private DecodeExecutors()
    {
    }

    public static ExecutorService newFixedCallerRuns(int threads, String threadNamePrefix)
    {
        if (threads <= 0)
        {
            throw new IllegalArgumentException("decode threads must be positive");
        }
        if (threadNamePrefix == null || threadNamePrefix.isBlank())
        {
            throw new IllegalArgumentException("threadNamePrefix must be non-blank");
        }

        AtomicInteger threadId = new AtomicInteger();
        return new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE),
                runnable -> new Thread(
                        runnable,
                        threadNamePrefix + "-" + threadId.incrementAndGet()),
                (task, executor) ->
                {
                    if (executor.isShutdown())
                    {
                        throw new RejectedExecutionException(
                                "Decode executor is shut down: " + threadNamePrefix);
                    }
                    task.run();
                });
    }

    public static void shutdownGracefully(
            ExecutorService executor, long timeoutSeconds, Logger logger)
    {
        if (executor == null)
        {
            return;
        }
        executor.shutdown();
        try
        {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS))
            {
                if (logger != null)
                {
                    logger.warn("Timed out waiting for decode executor to stop");
                }
                executor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
