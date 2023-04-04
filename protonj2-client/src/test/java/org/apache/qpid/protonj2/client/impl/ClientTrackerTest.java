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

package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Test the behavior of {@link ClientTracker}
 */
@Timeout(20)
public class ClientTrackerTest {

    @Test
    public void testCreateTrackerWithNullArgs() {
        assertThrows(NullPointerException.class, () -> new ClientTracker(null, null));
    }

    @Test
    public void testCreateTrackerWithNullDelivery() {
        final ClientSender sender = Mockito.mock(ClientSender.class);

        assertThrows(NullPointerException.class, () -> new ClientTracker(sender, null));
    }

    @Test
    public void testCreateTrackerWithNullSender() {
        final OutgoingDelivery delivery = Mockito.mock(OutgoingDelivery.class);

        assertThrows(NullPointerException.class, () -> new ClientTracker(null, delivery));
    }

    @Test
    public void testCreateTrackerAndCompleteSettlement() {
        final ClientSender sender = Mockito.mock(ClientSender.class);
        final OutgoingDelivery delivery = Mockito.mock(OutgoingDelivery.class);

        assertDoesNotThrow(() -> new ClientTracker(sender, delivery));

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<EventHandler<OutgoingDelivery>> deliveryUpdaterCapture = ArgumentCaptor.forClass(EventHandler.class);
        Mockito.verify(delivery).deliveryStateUpdatedHandler(deliveryUpdaterCapture.capture());

        assertNotNull(deliveryUpdaterCapture.getValue());
    }
}
