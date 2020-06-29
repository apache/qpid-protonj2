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
package org.apache.qpid.protonj2.engine.sasl.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.qpid.protonj2.types.Symbol;
import org.junit.Test;

public class SaslMechanismSelectorTest extends MechanismTestBase {

    private static final Symbol[] TEST_MECHANISMS_ARRAY = { ExternalMechanism.EXTERNAL,
                                                            CramMD5Mechanism.CRAM_MD5,
                                                            PlainMechanism.PLAIN,
                                                            AnonymousMechanism.ANONYMOUS };

    @Test
    public void testSelectAnonymousFromAll() {
        SaslMechanismSelector selector = new SaslMechanismSelector();

        Mechanism mech = selector.select(TEST_MECHANISMS_ARRAY, emptyCredentials());

        assertNotNull(mech);
        assertEquals(AnonymousMechanism.ANONYMOUS, mech.getName());
    }

    @Test
    public void testSelectPlain() {
        SaslMechanismSelector selector = new SaslMechanismSelector();

        Mechanism mech = selector.select(new Symbol[] { PlainMechanism.PLAIN, AnonymousMechanism.ANONYMOUS }, credentials());

        assertNotNull(mech);
        assertEquals(PlainMechanism.PLAIN, mech.getName());
    }

    @Test
    public void testSelectCramMD5() {
        SaslMechanismSelector selector = new SaslMechanismSelector();

        Mechanism mech = selector.select(TEST_MECHANISMS_ARRAY, credentials(USERNAME, PASSWORD));

        assertNotNull(mech);
        assertEquals(CramMD5Mechanism.CRAM_MD5, mech.getName());
    }

    @Test
    public void testSelectExternalIfPrincipalAvailable() {
        SaslMechanismSelector selector = new SaslMechanismSelector();

        Mechanism mech = selector.select(TEST_MECHANISMS_ARRAY, credentials());

        assertNotNull(mech);
        assertEquals(ExternalMechanism.EXTERNAL, mech.getName());
    }
}
