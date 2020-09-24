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
package org.apache.qpid.protonj2.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.junit.jupiter.api.Test;

public class ProtonDeliveryTagGeneratorTest {

    @Test
    public void testEmptyTagGenerator() {
        DeliveryTagGenerator tagGen1 = ProtonDeliveryTagGenerator.BUILTIN.EMPTY.createGenerator();
        DeliveryTagGenerator tagGen2 = ProtonDeliveryTagGenerator.BUILTIN.EMPTY.createGenerator();

        assertSame(tagGen1, tagGen2);

        DeliveryTag tag1 = tagGen1.nextTag();
        DeliveryTag tag2 = tagGen2.nextTag();

        assertSame(tag1, tag2);

        assertEquals(0, tag1.tagLength());
        assertNotNull(tag1.tagBytes());
    }
}
