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
package org.apache.qpid.protonj2.types.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;

public class CloseTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.CLOSE, new Close().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Close().toString());
    }

    @Test
    public void testCopyFromNew() {
        Close original = new Close();
        Close copy = original.copy();

        assertEquals(original.getError(), copy.getError());
    }

    @Test
    public void testCopyWithError() {
        Close original = new Close();
        original.setError(new ErrorCondition(AmqpError.DECODE_ERROR, "test"));

        Close copy = original.copy();

        assertNotSame(copy.getError(), original.getError());
        assertEquals(original.getError(), copy.getError());
    }
}
