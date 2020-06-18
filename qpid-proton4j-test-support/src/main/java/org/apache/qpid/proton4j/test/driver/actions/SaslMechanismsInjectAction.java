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
package org.apache.qpid.proton4j.test.driver.actions;

import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.codec.security.SaslMechanisms;
import org.apache.qpid.proton4j.types.Symbol;

/**
 * AMQP SaslMechanisms injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class SaslMechanismsInjectAction extends AbstractSaslPerformativeInjectAction<SaslMechanisms> {

    private final SaslMechanisms saslMechanisms = new SaslMechanisms();

    public SaslMechanismsInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    public SaslMechanismsInjectAction withMechanisms(String... saslServerMechanisms) {
        Symbol[] mechanisms = null;

        if (saslServerMechanisms != null) {
            mechanisms = new Symbol[saslServerMechanisms.length];
            for(int i = 0; i < saslServerMechanisms.length; ++i) {
                mechanisms[i] = Symbol.valueOf(saslServerMechanisms[i]);
            }
        }

        saslMechanisms.setSaslServerMechanisms(mechanisms);
        return this;
    }

    public SaslMechanismsInjectAction withMechanisms(Symbol... saslServerMechanisms) {
        saslMechanisms.setSaslServerMechanisms(saslServerMechanisms);
        return this;
    }

    @Override
    public SaslMechanisms getPerformative() {
        return saslMechanisms;
    }
}
