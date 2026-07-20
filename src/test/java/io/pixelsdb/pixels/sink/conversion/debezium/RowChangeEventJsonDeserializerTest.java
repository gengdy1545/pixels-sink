/*
 * Copyright 2025 PixelsDB.
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
 *
 */

package io.pixelsdb.pixels.sink.conversion.debezium;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowChangeEventJsonDeserializerTest
{
    private static final SchemaTableName REGION_TABLE =
            new SchemaTableName("pixels_realtime_crud", "region");
    private static TableMetadata previousRegionMetadata;
    private static boolean hadRegionMetadata;

    private final Deserializer<RowChangeEvent> deserializer = new RowChangeEventJsonDeserializer();
    private final Deserializer<SinkProto.TransactionMetadata> transactionDeserializer =
            new TransactionMetadataJsonDeserializer();

    @BeforeAll
    static void setUpConfig() throws Exception
    {
        PixelsSinkConfigFactory.reset();
        PixelsSinkConfigFactory.initialize(Objects.requireNonNull(
                RowChangeEventJsonDeserializerTest.class.getClassLoader()
                        .getResource("pixels-sink-test.properties")).getPath());
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        hadRegionMetadata = registry.containsKey(REGION_TABLE);
        previousRegionMetadata = registry.get(REGION_TABLE);
        registry.put(REGION_TABLE, regionMetadata());
    }

    @AfterAll
    static void resetConfig() throws ReflectiveOperationException
    {
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        if (hadRegionMetadata)
        {
            registry.put(REGION_TABLE, previousRegionMetadata);
        }
        else
        {
            registry.remove(REGION_TABLE);
        }
        PixelsSinkConfigFactory.reset();
    }

    private String loadSchemaFromFile(String filename) throws IOException, URISyntaxException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        return new String(Files.readAllBytes(Paths.get(
                Objects.requireNonNull(classLoader.getResource(filename)).toURI()
        )));
    }

    //   @ParameterizedTest
//    @EnumSource(value = OperationType.class, names = {"INSERT", "UPDATE"})
//        //, , "SNAPSHOT"
//    void shouldParseValidOperations(OperationType opType) throws Exception {
//        String jsonData = loadSchemaFromFile("records/" + opType.name().toLowerCase() + ".json");
//        RowChangeEvent event = deserializer.deserialize("test_topic", jsonData.getBytes());
//
//        assertNotNull(event);
//        assertEquals(opType, event.getOp());
//        assertEquals("region", event.getTable());
//
//        Map<String, Object> data = opType == OperationType.DELETE ?
//                event.getBeforeData() : event.getAfterData();
//        assertNotNull(data);
//    }

    @Test
    void shouldHandleDeleteOperation() throws Exception
    {
        String jsonData = loadSchemaFromFile("records/delete.json");
        RowChangeEvent event = deserializer.deserialize("test_topic", jsonData.getBytes());

        assertTrue(event.isDelete());
//        assertNotNull(event.getBeforeData());
//        assertNull(event.getAfterData());
    }


    @Test
    void shouldHandleEmptyData()
    {
        RowChangeEvent event = deserializer.deserialize("empty_topic", new byte[0]);
        assertNull(event);
    }

    @Test
    void shouldDeserializeTransactionMetadataWithConfiguredConnector()
    {
        String json = """
                {
                  "payload": {
                    "status": "END",
                    "id": "mysql-tx-1",
                    "event_count": 1,
                    "ts_ms": 1750000000000,
                    "data_collections": [
                      {"data_collection": "pixels_realtime_crud.region", "event_count": 1}
                    ]
                  }
                }
                """;

        SinkProto.TransactionMetadata transaction = transactionDeserializer.deserialize(
                "transaction", json.getBytes());

        assertEquals(SinkProto.TransactionStatus.END, transaction.getStatus());
        assertEquals("mysql-tx-1", transaction.getId());
        assertEquals("pixels_realtime_crud.region",
                transaction.getDataCollections(0).getDataCollection());
    }

    private static TableMetadata regionMetadata() throws Exception
    {
        Table table = new Table();
        table.setId(2);
        table.setName("region");
        return new TableMetadata(table, null, List.of(
                column("R_REGIONKEY", "int"),
                column("R_NAME", "string"),
                column("R_COMMENT", "string")));
    }

    private static Column column(String name, String type)
    {
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        return column;
    }

    @SuppressWarnings("unchecked")
    private static Map<SchemaTableName, TableMetadata> metadataRegistry()
            throws ReflectiveOperationException
    {
        Field registry = TableMetadataRegistry.class.getDeclaredField("registry");
        registry.setAccessible(true);
        return (Map<SchemaTableName, TableMetadata>) registry.get(TableMetadataRegistry.Instance());
    }
}

