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
package org.apache.qpid.protonj2.types.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.DeliveryState.DeliveryStateType;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.junit.jupiter.api.Test;

public class RejectedTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Rejected().toString());
    }

    @Test
    public void testGetErrorFromEmptySection() {
        assertNull(new Rejected().getError());
    }

    @Test
    public void testSetError() {
        assertNull(new Rejected().getError());
        ErrorCondition condition = new ErrorCondition(AmqpError.DECODE_ERROR, "Failed");
        Rejected rejected = new Rejected();
        rejected.setError(condition);
        assertNotNull(condition);
        assertSame(condition, rejected.getError());
    }

    @Test
    public void testGetType() {
        assertEquals(DeliveryStateType.Rejected, new Rejected().getType());
    }
}
