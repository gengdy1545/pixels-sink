/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the license, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionProcessorTest
{
    @Test
    void shouldForwardTransactionsAndStop() throws Exception
    {
        BlockingBoundedQueue<SinkProto.TransactionMetadata> queue =
                new BlockingBoundedQueue<>(2);
        RecordingWriter writer = new RecordingWriter();
        TransactionProcessor processor = new TransactionProcessor(queue, writer);
        Thread processorThread = new Thread(processor, "transaction-processor-test");
        SinkProto.TransactionMetadata first = SinkProto.TransactionMetadata.newBuilder()
                .setId("transaction-1")
                .build();
        SinkProto.TransactionMetadata second = SinkProto.TransactionMetadata.newBuilder()
                .setId("transaction-2")
                .build();

        try
        {
            processorThread.start();
            queue.put(first);
            queue.put(second);
            queue.close();
            processorThread.join(1000);

            assertTrue(writer.transactionsWritten.await(1, TimeUnit.SECONDS));
            assertEquals(List.of(first, second), writer.transactions);
            assertFalse(processorThread.isAlive());
        } finally
        {
            processor.abort();
            queue.abort();
            processorThread.interrupt();
            processorThread.join(1000);
        }
    }

    private static final class RecordingWriter implements PixelsSinkWriter
    {
        private final CountDownLatch transactionsWritten = new CountDownLatch(2);
        private final List<SinkProto.TransactionMetadata> transactions =
                new CopyOnWriteArrayList<>();

        @Override
        public void flush()
        {
        }

        @Override
        public boolean writeRow(io.pixelsdb.pixels.sink.event.RowChangeEvent rowChangeEvent)
        {
            return false;
        }

        @Override
        public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
        {
            transactions.add(transactionMetadata);
            transactionsWritten.countDown();
            return true;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
