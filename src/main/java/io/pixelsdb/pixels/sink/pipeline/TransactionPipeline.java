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
 * You should have received a copy of the GNU General Public License
 * along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.pipeline;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.provider.TransactionEventProvider;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;

public final class TransactionPipeline implements AutoCloseable
{
    private final TransactionEventProvider provider;
    private final PixelsSinkWriter writer;
    private final TransactionProcessor processor;
    private final Thread processorThread;

    public TransactionPipeline()
    {
        this.provider = new TransactionEventProvider();
        this.writer = PixelsSinkWriterFactory.getWriter();
        this.processor = new TransactionProcessor(provider, writer);
        this.processorThread = new Thread(processor, "transaction-processor");
    }

    public void start()
    {
        if (!processorThread.isAlive())
        {
            processorThread.start();
        }
    }

    public void publish(SinkProto.TransactionMetadata transaction)
    {
        provider.publishTransaction(transaction);
    }

    @Override
    public void close()
    {
        processor.stopProcessor();
        provider.close();
        processorThread.interrupt();
    }
}
