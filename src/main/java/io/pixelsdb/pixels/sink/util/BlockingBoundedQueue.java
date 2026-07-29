/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the license, or (at your option) any later version.
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

package io.pixelsdb.pixels.sink.util;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A bounded blocking queue with an explicit shutdown signal.
 *
 * <p>Closing the queue discards pending values and wakes a blocked consumer.
 * This is intended for pipeline shutdown, not graceful draining.</p>
 */
public final class BlockingBoundedQueue<T> implements Closeable
{
    private static final Object POISON_PILL = new Object();

    private final BlockingQueue<Object> queue;
    private volatile boolean closed;

    public BlockingBoundedQueue(int capacity)
    {
        if (capacity <= 0)
        {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public void put(T value)
    {
        if (value == null || closed)
        {
            return;
        }
        try
        {
            queue.put(value);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    public T take()
    {
        try
        {
            Object value = queue.take();
            if (value == POISON_PILL)
            {
                return null;
            }
            return (T) value;
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        queue.clear();
        queue.offer(POISON_PILL);
    }
}
