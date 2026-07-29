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

package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowChangeEventTest
{
    @Test
    void shouldReturnStableNonNegativeBucket()
    {
        ByteString indexKey = getIndexKey(0);
        int expectedBucket = RowChangeEvent.getBucketIdFromByteBuffer(indexKey);

        for (int i = 0; i < 10; ++i)
        {
            int bucket = RowChangeEvent.getBucketIdFromByteBuffer(indexKey);
            assertEquals(expectedBucket, bucket);
        }
        assertTrue(expectedBucket >= 0);
    }

    private static ByteString getIndexKey(int key)
    {
        int keySize = Integer.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        byteBuffer.putInt(key);
        return ByteString.copyFrom(byteBuffer.rewind());
    }
}
