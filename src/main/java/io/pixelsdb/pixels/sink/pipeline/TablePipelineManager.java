/*
 * Copyright 2026 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
