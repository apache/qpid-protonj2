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
package org.apache.qpid.proton4j.amqp.driver.actions;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslInit;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.Symbol;

/**
 * AMQP SaslInit injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class SaslInitInjectAction extends AbstractSaslPerformativeInjectAction<SaslInit> {

    private final SaslInit saslInit = new SaslInit();

    public SaslInitInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    public SaslInitInjectAction withMechanism(String mechanism) {
        saslInit.setMechanism(Symbol.valueOf(mechanism));
        return this;
    }

    public SaslInitInjectAction withMechanism(Symbol mechanism) {
        saslInit.setMechanism(mechanism);
        return this;
    }

    public SaslInitInjectAction withMechanism(ProtonBuffer response) {
        saslInit.setInitialResponse(new Binary(response));
        return this;
    }

    public SaslInitInjectAction withMechanism(Binary response) {
        saslInit.setInitialResponse(response);
        return this;
    }

    public SaslInitInjectAction withHostname(String hostname) {
        saslInit.setHostname(hostname);
        return this;
    }

    @Override
    public SaslInit getPerformative() {
        return saslInit;
    }
}
