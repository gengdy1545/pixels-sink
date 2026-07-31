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

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class PipelineTest
{
    @Test
    void shouldStartAndCloseTablePipeline() throws Exception
    {
        TestConfig.initializeUnitConfig();

        assertDoesNotThrow(() -> {
            try (TablePipeline pipeline = new TablePipeline())
            {
                pipeline.start();
            }
        });
    }

    @Test
    void shouldStartTransactionPipelineOnlyOnce() throws Exception
    {
        TestConfig.initializeUnitConfig();

        assertDoesNotThrow(() -> {
            try (TransactionPipeline pipeline = new TransactionPipeline())
            {
                pipeline.start();
                pipeline.start();
                pipeline.publish(SinkProto.TransactionMetadata.newBuilder()
                        .setId("transaction-1")
                        .setStatus(SinkProto.TransactionStatus.END)
                        .build());
            }
        });
    }

    @Test
    void shouldAbortPipelines() throws Exception
    {
        TestConfig.initializeUnitConfig();

        assertDoesNotThrow(() -> {
            try (TablePipeline tablePipeline = new TablePipeline();
                 TransactionPipeline transactionPipeline = new TransactionPipeline())
            {
                tablePipeline.start();
                transactionPipeline.start();
                tablePipeline.abort();
                transactionPipeline.abort();
            }
        });
    }

}
