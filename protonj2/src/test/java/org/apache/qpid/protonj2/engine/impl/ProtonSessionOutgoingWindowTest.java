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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import org.apache.qpid.protonj2.types.transport.Begin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test the proton outgoing window class for specification compliant behavior
 */
@ExtendWith(MockitoExtension.class)
class ProtonSessionOutgoingWindowTest {

    private static final int DEFAULT_MAX_FRAME_SIZE = 100_000;

    @Mock
    ProtonEngineConfiguration configuration;

    @Mock
    ProtonEngine engine;

    @Mock
    ProtonConnection connection;

    @Mock
    ProtonSession session;

    @Mock
    ProtonOutgoingDelivery delivery;

    ProtonSessionOutgoingWindow window;

    final Begin remoteBegin = new Begin();
    final Begin localBegin = new Begin();

    @BeforeEach
    public void setUp() throws Exception {
        when(configuration.getOutboundMaxFrameSize()).thenReturn((long) DEFAULT_MAX_FRAME_SIZE);
        when(engine.configuration()).thenReturn(configuration);
        when(session.getEngine()).thenReturn(engine);
        when(session.getConnection()).thenReturn(connection);

        window = new ProtonSessionOutgoingWindow(session);
    }

    @Test
    public void testConfigureOutbound() {
        window.configureOutbound(localBegin);
        window.handleBegin(remoteBegin);

        assertEquals(Integer.MAX_VALUE, localBegin.getOutgoingWindow());
        assertEquals(0, localBegin.getNextOutgoingId());

        assertFalse(window.isSendable());
        assertEquals(0, window.getNextOutgoingId());
        assertEquals(-1, window.getOutgoingCapacity()); // No limit set
    }
}
