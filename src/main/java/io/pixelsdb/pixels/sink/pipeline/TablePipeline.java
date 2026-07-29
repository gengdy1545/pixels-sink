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

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.processor.TableProcessor;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;

public final class TablePipeline implements AutoCloseable
{
    private final BlockingBoundedQueue<RowChangeEvent> eventQueue;
    private final PixelsSinkWriter writer;
    private final TableProcessor processor;

    public TablePipeline()
    {
        this.eventQueue = new BlockingBoundedQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE);
        this.writer = PixelsSinkWriterFactory.getWriter();
        this.processor = new TableProcessor(eventQueue, writer);
    }

    public void start()
    {
        processor.run();
    }

    public void publish(RowChangeEvent event)
    {
        eventQueue.put(event);
    }

    @Override
    public void close()
    {
        processor.stopProcessor();
        eventQueue.close();
    }
}
