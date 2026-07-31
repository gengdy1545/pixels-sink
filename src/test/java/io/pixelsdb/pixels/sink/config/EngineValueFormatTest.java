/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */

package io.pixelsdb.pixels.sink.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineValueFormatTest
{
    @Test
    void shouldDefaultToConnect()
    {
        assertEquals("connect", EngineValueFormat.resolve(null));
        assertEquals("connect", EngineValueFormat.resolve(""));
        assertEquals("connect", EngineValueFormat.resolve("CONNECT"));
    }

    @Test
    void shouldRejectNonConnectOnResolve()
    {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> EngineValueFormat.resolve("json"));
        assertTrue(error.getMessage().contains("not implemented"));
    }
}
