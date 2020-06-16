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
package org.apache.qpid.proton4j.types.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.messaging.TerminusExpiryPolicy;
import org.junit.Test;

public class TerminusExpiryPolicyTest {

    @Test
    public void testValueOf() {
        assertEquals(TerminusExpiryPolicy.valueOf(Symbol.valueOf("link-detach")), TerminusExpiryPolicy.LINK_DETACH);
        assertEquals(TerminusExpiryPolicy.valueOf(Symbol.valueOf("session-end")), TerminusExpiryPolicy.SESSION_END);
        assertEquals(TerminusExpiryPolicy.valueOf(Symbol.valueOf("connection-close")), TerminusExpiryPolicy.CONNECTION_CLOSE);
        assertEquals(TerminusExpiryPolicy.valueOf(Symbol.valueOf("never")), TerminusExpiryPolicy.NEVER);

        try {
            TerminusExpiryPolicy.valueOf((Symbol) null);
            fail("Should not accept null value");
        } catch (IllegalArgumentException iae) {}
    }
}
