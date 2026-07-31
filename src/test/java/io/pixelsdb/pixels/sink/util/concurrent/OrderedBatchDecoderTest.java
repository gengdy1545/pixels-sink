/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */

package io.pixelsdb.pixels.sink.util.concurrent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderedBatchDecoderTest
{
    private ExecutorService executor;

    @AfterEach
    void tearDown()
    {
        if (executor != null)
        {
            DecodeExecutors.shutdownGracefully(executor, 5, null);
        }
    }

    @Test
    void shouldPreserveInputOrder() throws Exception
    {
        executor = DecodeExecutors.newFixedCallerRuns(4, "ordered-batch-test");
        List<Integer> input = List.of(3, 1, 2);
        List<Integer> decoded = OrderedBatchDecoder.decodeInOrder(
                executor, input, value ->
                {
                    try
                    {
                        Thread.sleep(5L * value);
                    } catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return value * 10;
                });
        assertEquals(List.of(30, 10, 20), decoded);
    }

    @Test
    void shouldReplaceFailedRecordsWithNull() throws Exception
    {
        executor = DecodeExecutors.newFixedCallerRuns(2, "ordered-batch-fail");
        AtomicInteger calls = new AtomicInteger();
        List<Integer> decoded = OrderedBatchDecoder.decodeInOrder(
                executor,
                List.of(1, 2, 3),
                value ->
                {
                    calls.incrementAndGet();
                    if (value == 2)
                    {
                        throw new IllegalStateException("boom");
                    }
                    return value;
                });
        assertEquals(3, calls.get());
        assertEquals(1, decoded.get(0));
        assertNull(decoded.get(1));
        assertEquals(3, decoded.get(2));
    }
}
