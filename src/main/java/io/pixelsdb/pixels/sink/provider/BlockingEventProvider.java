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

package io.pixelsdb.pixels.sink.provider;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Bounded channel for already converted sink events.
 *
 * <p>The provider deliberately does not know how an event was read or
 * converted. Source adapters publish canonical objects and processors consume
 * them from this channel.</p>
 */
public class BlockingEventProvider<T> implements Closeable
{
    private static final Object POISON_PILL = new Object();

    private final BlockingQueue<Object> queue =
            new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE);
    private volatile boolean closed;

    public void publish(T event)
    {
        if (event == null || closed)
        {
            return;
        }
        try
        {
            queue.put(event);
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
