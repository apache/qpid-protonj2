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
import org.apache.qpid.proton4j.test.driver.codec.primitives.Binary;
import org.apache.qpid.proton4j.test.driver.codec.security.SaslResponse;

/**
 * AMQP SaslResponse injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class SaslResponseInjectAction extends AbstractSaslPerformativeInjectAction<SaslResponse> {

    private final SaslResponse saslResponse = new SaslResponse();

    public SaslResponseInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    public SaslResponseInjectAction withResponse(byte[] response) {
        saslResponse.setResponse(new Binary(response));
        return this;
    }

    public SaslResponseInjectAction withResponse(Binary response) {
        saslResponse.setResponse(response);
        return this;
    }

    @Override
    public SaslResponse getPerformative() {
        return saslResponse;
    }
}
