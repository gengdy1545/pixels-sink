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

public class StreamingSinkStorageSource extends AbstractSinkStorageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingSinkStorageSource.class);

    @Override
    public void start()
    {
        beginProcessing();
        try
        {
            for (String file : files)
            {
                if (!isRunning())
                {
                    break;
                }
                Storage.Scheme scheme = Storage.Scheme.fromPath(file);
                readers.add(PhysicalReaderUtil.newPhysicalReader(scheme, file));
            }

            do
            {
                for (PhysicalReader reader : readers)
                {
                    if (!isRunning())
                    {
                        break;
                    }

                    LOGGER.info("Start reading {}", reader.getPath());
                    reader.seek(0);
                    long offset = 0;
                    long fileLength = reader.getFileLength();
                    while (isRunning() && offset < fileLength)
                    {
                        Pair<Integer, ByteBuffer> record = readRecord(reader, offset, fileLength);
                        int valueLength = record.getRight().remaining();
                        submitRecord(record.getLeft(), record.getRight(), loopId);
                        offset += RECORD_HEADER_SIZE + (long) valueLength;
                    }
                }
                ++loopId;
            } while (storageLoopEnabled && isRunning());
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to read sink proto storage", e);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } finally
        {
            clean();
        }
    }
}
