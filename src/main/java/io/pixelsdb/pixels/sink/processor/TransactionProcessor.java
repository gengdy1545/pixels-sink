/*
 * Copyright 2025 PixelsDB.
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
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionProcessor implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProcessor.class);
    private final PixelsSinkWriter sinkWriter;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue;

    public TransactionProcessor(
            BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue)
    {
        this(eventQueue, PixelsSinkWriterFactory.getWriter());
    }

    public TransactionProcessor(
            BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue,
            PixelsSinkWriter sinkWriter)
    {
        this.eventQueue = eventQueue;
        this.sinkWriter = sinkWriter;
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            SinkProto.TransactionMetadata transaction = eventQueue.take();
            if (transaction == null)
            {
                running.set(false);
                break;
            }
            sinkWriter.writeTrans(transaction);
        }
        LOGGER.info("Processor thread exited for transaction");
    }

    public void abort()
    {
        LOGGER.info("Aborting transaction processor");
        running.set(false);
    }
}
