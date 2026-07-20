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

package io.pixelsdb.pixels.sink.provider;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BlockingEventProviderTest
{
    @Test
    void shouldPublishAndTakeCanonicalEvent()
    {
        BlockingEventProvider<Integer> provider = new BlockingEventProvider<>();
        provider.publish(7);

        assertEquals(7, provider.take());
        provider.close();
    }

    @Test
    void shouldWakeConsumerWhenClosed()
            throws Exception
    {
        BlockingEventProvider<Integer> provider = new BlockingEventProvider<>();
        CountDownLatch finished = new CountDownLatch(1);
        Thread consumer = new Thread(() ->
        {
            assertNull(provider.take());
            finished.countDown();
        });
        consumer.start();

        provider.close();

        assertEquals(true, finished.await(1, TimeUnit.SECONDS));
        consumer.join();
    }
}
