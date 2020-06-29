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

import static org.junit.Assert.assertTrue;

import org.apache.qpid.protonj2.types.Symbol;
import org.junit.Test;

public class AbstractMechanismTest {

    @Test
    public void testToStringCarriesMechName() {
        TestMechanism mech = new TestMechanism();

        assertTrue(mech.toString().contains("TEST"));
    }

    private static class TestMechanism extends AbstractMechanism {

        @Override
        public Symbol getName() {
            return Symbol.valueOf("TEST");
        }

        @Override
        public boolean isApplicable(SaslCredentialsProvider credentials) {
            return true;
        }
    }
}
