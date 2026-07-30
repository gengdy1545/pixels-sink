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

package io.pixelsdb.pixels.sink.conversion.sinkproto;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RowRecordConverterTest
{
    private static final SchemaTableName TABLE =
            new SchemaTableName("storage_test", "records");
    private Map<SchemaTableName, TableMetadata> registry;
    private TableMetadata previousMetadata;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception
    {
        Field field = TableMetadataRegistry.class.getDeclaredField("registry");
        field.setAccessible(true);
        registry = (Map<SchemaTableName, TableMetadata>)
                field.get(TableMetadataRegistry.Instance());
        previousMetadata = registry.put(TABLE, metadata());
    }

    @AfterEach
    void tearDown()
    {
        if (previousMetadata == null)
        {
            registry.remove(TABLE);
        } else
        {
            registry.put(TABLE, previousMetadata);
        }
    }

    @Test
    void shouldConvertCanonicalRowRecordIntoRuntimeEvent()
            throws Exception
    {
        SinkProto.RowRecord record = SinkProto.RowRecord.newBuilder()
                .setOp(SinkProto.OperationType.INSERT)
                .setSource(SinkProto.SourceInfo.newBuilder()
                        .setDb(TABLE.getSchemaName())
                        .setTable(TABLE.getTableName()))
                .setAfter(SinkProto.RowValue.newBuilder()
                        .addValues(SinkProto.ColumnValue.newBuilder()
                                .setValue(com.google.protobuf.ByteString.EMPTY))
                        .build())
                .build();

        RowChangeEvent event =
                new RowRecordConverter(TableMetadataRegistry.Instance()).convert(record);

        assertEquals(record, event.getRowRecord());
        assertEquals(TABLE.getSchemaName(), event.getDb());
        assertEquals(TABLE.getTableName(), event.getTable());
    }

    private TableMetadata metadata() throws Exception
    {
        Table table = new Table();
        table.setId(1);
        table.setName(TABLE.getTableName());
        Column column = new Column();
        column.setName("id");
        column.setType("int");
        return new TableMetadata(table, null, List.of(column));
    }
}
