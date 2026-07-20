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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.util.TestDateUtil;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebeziumRowValueConverterTest
{
    // BqQ= 17
    // JbR7 24710.35
//    @ParameterizedTest
//    @CsvSource({
//            // encodedValue, expectedValue, precision, scale
//            "BqQ=, 17.00, 15, 2",
//            "JbR7, 24710.35, 15, 2",
//    })
//    void testParseDecimalValid(String encodedValue, String expectedValue, int precision, int scale) {
//        JsonNode node = new TextNode(encodedValue);
//        TypeDescription type = TypeDescription.createDecimal(precision, scale);
//        DebeziumRowValueConverter rowDataParser = new DebeziumRowValueConverter(type);
//        BigDecimal result = rowDataParser.parseDecimal(node, type);
//        assertEquals(new BigDecimal(expectedValue), result);
//    }

    @Test
    void shouldEncodeCanonicalStructValues() throws Exception
    {
        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of("id", "name", "amount", "note", "empty_value"),
                List.of("bigint", "varchar(64)", "decimal(10,2)", "varchar(64)", "varchar(64)"));
        Schema rowSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("amount", Decimal.builder(2)
                        .parameter("connect.decimal.precision", "10").build())
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .field("empty_value", Schema.STRING_SCHEMA)
                .build();
        Struct row = new Struct(rowSchema)
                .put("id", 9223372036854775806L)
                .put("name", "TDSQL value  ")
                .put("amount", new BigDecimal("24710.35"))
                .put("note", null)
                .put("empty_value", "");

        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(row, builder);
        SinkProto.RowValue value = builder.build();

        assertEquals(9223372036854775806L,
                ByteBuffer.wrap(value.getValues(0).getValue().toByteArray()).getLong());
        assertEquals("TDSQL value  ", value.getValues(1).getValue().toStringUtf8());
        assertEquals("24710.35", value.getValues(2).getValue().toStringUtf8());
        assertTrue(value.getValues(3).getIsNull());
        assertEquals(0, value.getValues(3).getValue().size());
        assertFalse(value.getValues(4).getIsNull());
        assertEquals("", value.getValues(4).getValue().toStringUtf8());

        ObjectNode jsonRow = new ObjectMapper().createObjectNode();
        jsonRow.put("id", 9223372036854775806L);
        jsonRow.put("name", "TDSQL value  ");
        jsonRow.put("amount", Base64.getEncoder().encodeToString(
                new BigDecimal("24710.35").unscaledValue().toByteArray()));
        jsonRow.putNull("note");
        jsonRow.put("empty_value", "");
        SinkProto.RowValue.Builder jsonBuilder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(jsonRow, jsonBuilder);

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "CanonicalRow",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "amount", "type": "bytes"},
                    {"name": "note", "type": ["null", "string"], "default": null},
                    {"name": "empty_value", "type": "string"}
                  ]
                }
                """);
        GenericData.Record avroRow = new GenericData.Record(avroSchema);
        avroRow.put("id", 9223372036854775806L);
        avroRow.put("name", "TDSQL value  ");
        avroRow.put("amount", new BigDecimal("24710.35"));
        avroRow.put("note", null);
        avroRow.put("empty_value", "");
        SinkProto.RowValue.Builder avroBuilder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(avroRow, avroBuilder);

        assertEquals(value, jsonBuilder.build());
        assertEquals(value, avroBuilder.build());
    }

    @Test
    void shouldPreserveExistingIntegerWidths() throws Exception
    {
        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of("tiny", "small", "integer"),
                List.of("tinyint", "smallint", "integer"));
        Schema rowSchema = SchemaBuilder.struct()
                .field("tiny", Schema.INT8_SCHEMA)
                .field("small", Schema.INT16_SCHEMA)
                .field("integer", Schema.INT32_SCHEMA)
                .build();
        Struct row = new Struct(rowSchema)
                .put("tiny", (byte) 7)
                .put("small", (short) 1024)
                .put("integer", 65536);

        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(row, builder);

        assertEquals(1, builder.getValues(0).getValue().size());
        assertEquals(2, builder.getValues(1).getValue().size());
        assertEquals(4, builder.getValues(2).getValue().size());
    }

    @Test
    void testParseDate()
    {
        int day = 17059;
        Date debeziumDate = TestDateUtil.fromDebeziumDate(day);
        String dayString = TestDateUtil.convertDateToDayString(debeziumDate);
        long ts = 1473927308302000L;
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts / 1000), ZoneOffset.UTC);
        ZonedDateTime zonedDateTime = Instant.ofEpochMilli(ts).atZone(ZoneOffset.UTC);
        boolean pause = true;
    }
}
