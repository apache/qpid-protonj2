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

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ProtonOutgoingDeliveryTest extends ProtonEngineTestSupport {

    public static final int DEFAULT_MESSAGE_FORMAT = 0;

    @Test
    public void testToStringOnEmptyDeliveryDoesNotNPE() throws Exception {
        ProtonOutgoingDelivery delivery = new ProtonOutgoingDelivery(Mockito.mock(ProtonSender.class));
        assertNotNull(delivery.toString());
    }

    @Test
    public void testDefaultMessageFormat() throws Exception {
        ProtonOutgoingDelivery delivery = new ProtonOutgoingDelivery(Mockito.mock(ProtonSender.class));

        assertEquals(0L, DEFAULT_MESSAGE_FORMAT, "Unexpected value");
        assertEquals(DEFAULT_MESSAGE_FORMAT, delivery.getMessageFormat(), "Unexpected message format");
    }

    @Test
    public void testSetGetMessageFormat() throws Exception {
        ProtonOutgoingDelivery delivery = new ProtonOutgoingDelivery(Mockito.mock(ProtonSender.class));

        // lowest value and default
        int newFormat = 0;
        delivery.setMessageFormat(newFormat);
        assertEquals(newFormat, delivery.getMessageFormat(), "Unexpected message format");

        newFormat = 123456;
        delivery.setMessageFormat(newFormat);
        assertEquals(newFormat, delivery.getMessageFormat(), "Unexpected message format");

        // Highest value
        newFormat = UnsignedInteger.MAX_VALUE.intValue();
        delivery.setMessageFormat(newFormat);
        assertEquals(newFormat, delivery.getMessageFormat(), "Unexpected message format");
    }
}
