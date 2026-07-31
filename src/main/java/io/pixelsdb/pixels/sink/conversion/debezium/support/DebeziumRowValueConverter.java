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
 */
package io.pixelsdb.pixels.sink.conversion.debezium.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DebeziumRowValueConverter
{
    private final TypeDescription schema;

    public DebeziumRowValueConverter(TypeDescription schema)
    {
        this.schema = schema;
    }

    private static void buildFloat32(float value, SinkProto.ColumnValue.Builder columnValueBuilder)
    {
        buildInt32(Float.floatToIntBits(value), 4, columnValueBuilder);
    }

    private static void buildInt32(int value, int capacity, SinkProto.ColumnValue.Builder columnValueBuilder)
    {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        switch (capacity)
        {
            case Byte.BYTES -> buffer.put((byte) value);
            case Short.BYTES -> buffer.putShort((short) value);
            case Integer.BYTES -> buffer.putInt(value);
            default -> throw new IllegalArgumentException("Unsupported integer width: " + capacity);
        }
        byte[] bytes = buffer.array();
        columnValueBuilder.setValue(ByteString.copyFrom(bytes));
    }

    public void parse(GenericRecord record, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseCanonicalValue(record.get(fieldName), fieldType).build());
        }
    }

    public void parse(JsonNode node, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseValue(node.get(fieldName), fieldName, fieldType).build());
        }
    }

    public void parse(Struct record, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            if (record.schema().field(fieldName) == null)
            {
                throw new IllegalArgumentException("Missing field in Debezium row schema: " + fieldName);
            }
            builder.addValues(parseCanonicalValue(record.get(fieldName), fieldType).build());
        }
    }

    private SinkProto.ColumnValue.Builder parseValue(JsonNode valueNode, String fieldName, TypeDescription type)
    {
        if (valueNode == null || valueNode.isNull())
        {
            return SinkProto.ColumnValue.newBuilder()
                    // .setName(fieldName)
                    .setValue(ByteString.EMPTY);
        }

        SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder();

        switch (type.getCategory())
        {
            case BYTE:
            {
                buildInt32(valueNode.asInt(), Byte.BYTES, columnValueBuilder);
                break;
            }
            case SHORT:
            {
                buildInt32(valueNode.asInt(), Short.BYTES, columnValueBuilder);
                break;
            }
            case INT:
            {
                buildInt32(valueNode.asInt(), Integer.BYTES, columnValueBuilder);
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.INT));
                break;
            }
            case LONG:
            {
                long value = valueNode.asLong();
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.LONG));
                break;
            }
            case CHAR:
            {
                String text = valueNode.asText();
                byte[] bytes = new byte[]{(byte) text.charAt(0)};
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
//                        .setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case VARCHAR:
            case STRING:
            case VARBINARY:
            {
                String value = valueNode.asText();
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case DECIMAL:
            {
                String value = parseDecimal(valueNode, type).toString();
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
//                        .setKind(PixelsProto.Type.Kind.DECIMAL)
//                        .setDimension(type.getPrecision())
//                        .setScale(type.getScale()));
                break;
            }
            case BINARY:
            {
                String base64 = valueNode.asText(); // assume already base64 encoded
                columnValueBuilder.setValue(ByteString.copyFrom(base64, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.BINARY));
                break;
            }
            case STRUCT:
            {
                // You can recursively parse fields in a struct here
                throw new UnsupportedOperationException("STRUCT parsing not yet implemented");
            }
            case DOUBLE:
            {
                double value = valueNode.asDouble();
                long longBits = Double.doubleToLongBits(value);
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(longBits).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.DOUBLE));
                break;
            }
            case FLOAT:
            {
                float value = (float) valueNode.asDouble();
                buildFloat32(value, columnValueBuilder);
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.FLOAT));
                break;
            }
            case DATE:
            case TIME:
            {
                buildInt32(valueNode.asInt(), Integer.BYTES, columnValueBuilder);
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                //        .setKind(PixelsProto.Type.Kind.DATE));
                break;
            }
            case TIMESTAMP:
            {
                long timestamp = valueNode.asLong();
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(timestamp).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                //        .setKind(PixelsProto.Type.Kind.DATE));
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + type.getCategory());
        }

        return columnValueBuilder;
    }

    private SinkProto.ColumnValue.Builder parseCanonicalValue(Object raw, TypeDescription type)
    {
        if (raw == null)
        {
            return SinkProto.ColumnValue.newBuilder()
                    .setValue(ByteString.EMPTY);
        }
        if (raw instanceof JsonNode jsonNode)
        {
            return parseValue(jsonNode, "", type);
        }

        SinkProto.ColumnValue.Builder builder = SinkProto.ColumnValue.newBuilder();
        switch (type.getCategory())
        {
            case BYTE:
                buildInt32(((Number) raw).intValue(), Byte.BYTES, builder);
                break;
            case SHORT:
                buildInt32(((Number) raw).intValue(), Short.BYTES, builder);
                break;
            case INT:
                buildInt32(((Number) raw).intValue(), Integer.BYTES, builder);
                break;
            case LONG:
            case TIMESTAMP:
            {
                byte[] bytes = ByteBuffer.allocate(Long.BYTES)
                        .putLong(((Number) raw).longValue()).array();
                builder.setValue(ByteString.copyFrom(bytes));
                break;
            }
            case DATE:
            case TIME:
                buildInt32(((Number) raw).intValue(), Integer.BYTES, builder);
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                builder.setValue(ByteString.copyFrom(raw.toString(), StandardCharsets.UTF_8));
                break;
            case DECIMAL:
            {
                BigDecimal decimal = toBigDecimal(raw, type);
                builder.setValue(ByteString.copyFrom(
                        decimal.toPlainString(), StandardCharsets.UTF_8));
                break;
            }
            case BINARY:
            case VARBINARY:
                builder.setValue(ByteString.copyFrom(toBytes(raw)));
                break;
            case DOUBLE:
            {
                long bits = Double.doubleToLongBits(((Number) raw).doubleValue());
                builder.setValue(ByteString.copyFrom(
                        ByteBuffer.allocate(Long.BYTES).putLong(bits).array()));
                break;
            }
            case FLOAT:
                buildFloat32(((Number) raw).floatValue(), builder);
                break;
            case BOOLEAN:
                builder.setValue(ByteString.copyFrom(
                        new byte[]{(byte) ((Boolean) raw ? 1 : 0)}));
                break;
            case STRUCT:
                throw new UnsupportedOperationException("STRUCT parsing not yet implemented");
            default:
                throw new IllegalArgumentException(
                        "Unsupported canonical type: " + type.getCategory());
        }
        return builder;
    }

    private BigDecimal toBigDecimal(Object raw, TypeDescription type)
    {
        if (raw instanceof BigDecimal decimal)
        {
            return decimal;
        }
        if (raw instanceof ByteBuffer buffer)
        {
            return new BigDecimal(new BigInteger(toBytes(buffer)), type.getScale());
        }
        if (raw instanceof byte[] bytes)
        {
            return new BigDecimal(new BigInteger(bytes), type.getScale());
        }
        return new BigDecimal(raw.toString());
    }

    private byte[] toBytes(Object raw)
    {
        if (raw instanceof byte[] bytes)
        {
            return bytes;
        }
        if (raw instanceof ByteBuffer buffer)
        {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        return raw.toString().getBytes(StandardCharsets.UTF_8);
    }

    BigDecimal parseDecimal(JsonNode node, TypeDescription type)
    {
        byte[] bytes = Base64.getDecoder().decode(node.asText());
        int scale = type.getScale();
        return new BigDecimal(new BigInteger(bytes), scale);
    }

}
