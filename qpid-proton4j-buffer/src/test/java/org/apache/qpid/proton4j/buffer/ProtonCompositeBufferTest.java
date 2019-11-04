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
package org.apache.qpid.proton4j.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Test the Proton Composite Buffer class
 */
public class ProtonCompositeBufferTest extends ProtonAbstractBufferTest {

    @Test
    public void testCreateDefaultCompositeBuffer() {
        ProtonCompositeBuffer composite = new ProtonCompositeBuffer(Integer.MAX_VALUE);
        assertNotNull(composite);
        assertEquals(0, composite.capacity());
        assertEquals(Integer.MAX_VALUE, composite.maxCapacity());
    }

    @Override
    protected ProtonBuffer allocateDefaultBuffer() {
        return new ProtonCompositeBuffer(Integer.MAX_VALUE).capacity(DEFAULT_CAPACITY);
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity) {
        return new ProtonCompositeBuffer(Integer.MAX_VALUE).capacity(initialCapacity);
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonCompositeBuffer(maxCapacity).capacity(initialCapacity);
    }

    @Override
    protected ProtonBuffer wrapBuffer(byte[] array) {
        ProtonCompositeBuffer composite = new ProtonCompositeBuffer(Integer.MAX_VALUE);
        return composite.addBuffer(ProtonByteBufferAllocator.DEFAULT.wrap(array));
    }
}
