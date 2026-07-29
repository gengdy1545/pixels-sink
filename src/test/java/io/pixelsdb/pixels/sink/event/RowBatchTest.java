/*
 * Copyright 2025 PixelsDB.
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

package io.pixelsdb.pixels.sink.event;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RowBatchTest
{
    @Test
    void integerRowBatchTest()
    {
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                java.util.List.of("id"), java.util.List.of("int"));
        VectorizedRowBatch rowBatch = schema.createRowBatch(
                3, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        IntColumnVector vector = (IntColumnVector) rowBatch.cols[0];
        vector.add(10);
        vector.add(20);
        vector.add(30);
        rowBatch.size = 3;

        VectorizedRowBatch newRowBatch = VectorizedRowBatch.deserialize(rowBatch.serialize());

        assertEquals(3, newRowBatch.size);
        IntColumnVector newVector = (IntColumnVector) newRowBatch.cols[0];
        assertEquals(10, newVector.vector[0]);
        assertEquals(20, newVector.vector[1]);
        assertEquals(30, newVector.vector[2]);
    }

    @Test
    void varcharRowBatchTest()
    {
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                java.util.List.of("name"), java.util.List.of("varchar(100)"));
        VectorizedRowBatch rowBatch = schema.createRowBatch(3, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        BinaryColumnVector v = (BinaryColumnVector) rowBatch.cols[0];
        v.add("rr");
        v.add("zz");
        v.add("rr");
        rowBatch.size = 3;

        VectorizedRowBatch newRowBatch = VectorizedRowBatch.deserialize(rowBatch.serialize());

        assertEquals(3, newRowBatch.size);
        BinaryColumnVector newVector = (BinaryColumnVector) newRowBatch.cols[0];
        assertEquals("rr", new String(newVector.vector[0], newVector.start[0], newVector.lens[0]));
        assertEquals("zz", new String(newVector.vector[1], newVector.start[1], newVector.lens[1]));
        assertEquals("rr", new String(newVector.vector[2], newVector.start[2], newVector.lens[2]));
    }
}
