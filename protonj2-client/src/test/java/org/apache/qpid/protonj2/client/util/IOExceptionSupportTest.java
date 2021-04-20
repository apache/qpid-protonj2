/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class IOExceptionSupportTest {

    @Test
    void testCreateFromEmptyException() {
        IOException ex = IOExceptionSupport.create(new RuntimeException());

        assertNotNull(ex.getMessage());
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertTrue(ex.getMessage().contains("RuntimeException"));
    }

    @Test
    void testCreateFromRuntimeException() {
        RuntimeException cause = new RuntimeException("Error");
        IOException ex = IOExceptionSupport.create(cause);

        assertNotNull(ex.getMessage());
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertSame(ex.getCause(), cause);
        assertEquals(ex.getMessage(), cause.getMessage());
    }

    @Test
    void testCreateFromRuntimeExceptionEmptyStrngInMessage() {
        RuntimeException cause = new RuntimeException("");
        IOException ex = IOExceptionSupport.create(cause);

        assertNotNull(ex.getMessage());
        assertFalse(ex.getMessage().isEmpty());
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertSame(ex.getCause(), cause);
        assertNotEquals(ex.getMessage(), cause.getMessage());
        assertTrue(ex.getMessage().contains("RuntimeException"));
    }
}
