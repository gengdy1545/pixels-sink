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
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.source.storage;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MemorySinkStorageSource extends AbstractSinkStorageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MemorySinkStorageSource.class);

    // All preloaded records, order preserved
    // key + value buffer
    private final List<Pair<Integer, ByteBuffer>> preloadedRecords = new ArrayList<>();

    @Override
    public void start()
    {
        beginProcessing();
        try
        {
            /* =====================================================
             * 1. Initialization phase: preload all ByteBuffers
             * ===================================================== */
            for (String file : files)
            {
                if (!isRunning())
                {
                    break;
                }
                Storage.Scheme scheme = Storage.Scheme.fromPath(file);
                LOGGER.info("Preloading file {}", file);

                PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(scheme, file);
                readers.add(reader);

                reader.seek(0);
                long offset = 0;
                long fileLength = reader.getFileLength();
                while (isRunning() && offset < fileLength)
                {
                    Pair<Integer, ByteBuffer> record = readRecord(reader, offset, fileLength);
                    int valueLength = record.getRight().remaining();
                    // Store into a single global array
                    ByteBuffer cleanBuffer = record.getRight().duplicate();
                    cleanBuffer.rewind();
                    cleanBuffer.limit(cleanBuffer.position() + valueLength);
                    preloadedRecords.add(new Pair<>(record.getLeft(), cleanBuffer));
                    offset += RECORD_HEADER_SIZE + (long) valueLength;
                }
            }

            LOGGER.info("Preload finished, total records = {}", preloadedRecords.size());

            /* =====================================================
             * Phase 2: Runtime loop
             * Queue initialization, consumer startup, and feeding
             * are done together in this phase
             * ===================================================== */
            do
            {
                for (Pair<Integer, ByteBuffer> record : preloadedRecords)
                {
                    if (!isRunning())
                    {
                        break;
                    }
                    int key = record.getLeft();
                    ByteBuffer src = record.getRight();
                    ByteBuffer copy = ByteBuffer.allocate(src.remaining());
                    copy.put(src.duplicate().rewind());
                    copy.flip();
                    submitRecord(key, copy, loopId);
                }
                ++loopId;
            } while (storageLoopEnabled && isRunning());
        } catch (IOException | IndexOutOfBoundsException e)
        {
            throw new RuntimeException(e);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } finally
        {
            clean();
        }
    }
}
