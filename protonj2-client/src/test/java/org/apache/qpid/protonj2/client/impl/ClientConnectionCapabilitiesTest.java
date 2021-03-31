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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.types.Symbol;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ClientConnectionCapabilitiesTest {

    public static final Symbol[] ANONYMOUS_RELAY = new Symbol[] { Symbol.valueOf("ANONYMOUS-RELAY") };
    public static final Symbol[] DELAYED_DELIVERY = new Symbol[] { Symbol.valueOf("DELAYED_DELIVERY") };
    public static final Symbol[] ANONYMOUS_RELAY_PLUS = new Symbol[] { Symbol.valueOf("ANONYMOUS-RELAY"),
                                                                       Symbol.valueOf("DELAYED_DELIVERY")};

    @Test
    void testAnonymousRelaySupportedIsFalseByDefault() {
        ClientConnectionCapabilities capabilities = new ClientConnectionCapabilities();

        assertFalse(capabilities.anonymousRelaySupported());
    }

    @Test
    public void testAnonymousRelaySupportedWhenBothIndicateInCapabilities() {
        doTestIsAnonymousRelaySupported(ANONYMOUS_RELAY, ANONYMOUS_RELAY, true);
    }

    @Test
    public void testAnonymousRelaySupportedWhenBothIndicateInCapabilitiesAlongWithOthers() {
        doTestIsAnonymousRelaySupported(ANONYMOUS_RELAY, ANONYMOUS_RELAY_PLUS, true);
    }

    @Test
    public void testAnonymousRelayNotSupportedWhenServerDoesNotAdvertiseIt() {
        doTestIsAnonymousRelaySupported(ANONYMOUS_RELAY, null, false);
    }

    @Test
    public void testAnonymousRelaySupportedWhenServerAdvertisesButClientDoesNotRequestIt() {
        doTestIsAnonymousRelaySupported(null, ANONYMOUS_RELAY, true);
    }

    @Test
    public void testAnonymousRelayNotSupportedWhenNeitherSideIndicatesIt() {
        doTestIsAnonymousRelaySupported(null, null, false);
    }

    private void doTestIsAnonymousRelaySupported(Symbol[] desired, Symbol[] offered, boolean expectation) {
        ClientConnectionCapabilities capabilities = new ClientConnectionCapabilities();

        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getDesiredCapabilities()).thenReturn(desired);
        Mockito.when(connection.getRemoteOfferedCapabilities()).thenReturn(offered);

        capabilities.determineCapabilities(connection);

        if (expectation) {
            assertTrue(capabilities.anonymousRelaySupported());
        } else {
            assertFalse(capabilities.anonymousRelaySupported());
        }
    }

    @Test
    public void testDelayedDeliverySupportedWhenBothIndicateInCapabilities() {
        doTestIsDelayedDeliverySupported(DELAYED_DELIVERY, DELAYED_DELIVERY, true);
    }

    @Test
    public void testDelayedDeliverySupportedWhenBothIndicateInCapabilitiesAlongWithOthers() {
        doTestIsDelayedDeliverySupported(DELAYED_DELIVERY, ANONYMOUS_RELAY_PLUS, true);
    }

    @Test
    public void testDelayedDeliveryNotSupportedWhenServerDoesNotAdvertiseIt() {
        doTestIsDelayedDeliverySupported(DELAYED_DELIVERY, null, false);
    }

    @Test
    public void testDelayedDeliverySupportedWhenServerAdvertisesButClientDoesNotRequestIt() {
        doTestIsDelayedDeliverySupported(null, DELAYED_DELIVERY, true);
    }

    @Test
    public void testDelayedDeliveryNotSupportedWhenNeitherSideIndicatesIt() {
        doTestIsDelayedDeliverySupported(null, null, false);
    }

    private void doTestIsDelayedDeliverySupported(Symbol[] desired, Symbol[] offered, boolean expectation) {
        ClientConnectionCapabilities capabilities = new ClientConnectionCapabilities();

        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getDesiredCapabilities()).thenReturn(desired);
        Mockito.when(connection.getRemoteOfferedCapabilities()).thenReturn(offered);

        capabilities.determineCapabilities(connection);

        if (expectation) {
            assertTrue(capabilities.deliveryDelaySupported());
        } else {
            assertFalse(capabilities.deliveryDelaySupported());
        }
    }
}
