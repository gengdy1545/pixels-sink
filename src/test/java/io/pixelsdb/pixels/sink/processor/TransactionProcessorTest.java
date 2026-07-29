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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionProcessorTest
{
    @Test
    void shouldForwardTransactionsAndStop() throws Exception
    {
        BlockingBoundedQueue<SinkProto.TransactionMetadata> queue =
                new BlockingBoundedQueue<>(1);
        RecordingWriter writer = new RecordingWriter();
        TransactionProcessor processor = new TransactionProcessor(queue, writer);
        Thread processorThread = new Thread(processor, "transaction-processor-test");
        SinkProto.TransactionMetadata transaction = SinkProto.TransactionMetadata.newBuilder()
                .setId("transaction-1")
                .build();

        try
        {
            processorThread.start();
            queue.put(transaction);

            assertTrue(writer.transactionWritten.await(1, TimeUnit.SECONDS));
            assertSame(transaction, writer.transaction);
        } finally
        {
            processor.stopProcessor();
            queue.close();
            processorThread.interrupt();
            processorThread.join(1000);
        }
        assertTrue(!processorThread.isAlive());
    }

    private static final class RecordingWriter implements PixelsSinkWriter
    {
        private final CountDownLatch transactionWritten = new CountDownLatch(1);
        private SinkProto.TransactionMetadata transaction;

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
            transaction = transactionMetadata;
            transactionWritten.countDown();
            return true;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
