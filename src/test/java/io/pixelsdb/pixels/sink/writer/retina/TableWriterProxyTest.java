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

package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.TestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("integration")
class TableWriterProxyTest
{
    private static final String TABLE_NAME = "test";

    @BeforeAll
    static void init() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
    }

    @Test
    void shouldReuseTableWriter() throws IOException
    {
        TableWriterProxy tableWriterProxy = TableWriterProxy.getInstance();
        TableWriter first = tableWriterProxy.getTableWriter(TABLE_NAME, 0, 0);

        for (int i = 0; i < 10; i++)
        {
            TableWriter tableWriter = tableWriterProxy.getTableWriter(TABLE_NAME, 0, 0);
            assertNotNull(tableWriter);
            assertSame(first, tableWriter);
        }
    }
}
