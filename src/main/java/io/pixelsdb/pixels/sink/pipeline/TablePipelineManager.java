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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TablePipelineManager implements AutoCloseable
{
    private final Map<SchemaTableName, TablePipeline> pipelines = new ConcurrentHashMap<>();

    public void route(RowChangeEvent event)
    {
        if (event == null)
        {
            return;
        }
        SchemaTableName table = new SchemaTableName(event.getSchemaName(), event.getTable());
        pipelines.computeIfAbsent(table, ignored ->
        {
            TablePipeline pipeline = new TablePipeline();
            pipeline.start();
            return pipeline;
        }).publish(event);
    }

    @Override
    public void close()
    {
        pipelines.values().forEach(TablePipeline::close);
        pipelines.clear();
    }
}
