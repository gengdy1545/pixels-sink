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

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TablePipelineManager implements AutoCloseable
{
    private final Map<SchemaTableName, TablePipeline> pipelines = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private volatile boolean closed;

    public void route(RowChangeEvent event)
    {
        if (event == null || closed)
        {
            return;
        }
        SchemaTableName table = new SchemaTableName(event.getSchemaName(), event.getTable());
        TablePipeline pipeline = pipelines.get(table);
        if (pipeline == null)
        {
            synchronized (lifecycleLock)
            {
                if (closed)
                {
                    return;
                }
                pipeline = pipelines.computeIfAbsent(table, ignored ->
                {
                    TablePipeline newPipeline = new TablePipeline();
                    newPipeline.start();
                    return newPipeline;
                });
            }
        }
        pipeline.publish(event);
    }

    @Override
    public void close()
    {
        List<TablePipeline> pipelinesToClose;
        synchronized (lifecycleLock)
        {
            if (closed)
            {
                return;
            }
            closed = true;
            pipelinesToClose = new ArrayList<>(pipelines.values());
            pipelines.clear();
        }
        pipelinesToClose.forEach(TablePipeline::close);
    }

    public void abort()
    {
        List<TablePipeline> pipelinesToAbort;
        synchronized (lifecycleLock)
        {
            if (closed)
            {
                return;
            }
            closed = true;
            pipelinesToAbort = new ArrayList<>(pipelines.values());
            pipelines.clear();
        }
        pipelinesToAbort.forEach(TablePipeline::abort);
    }
}
