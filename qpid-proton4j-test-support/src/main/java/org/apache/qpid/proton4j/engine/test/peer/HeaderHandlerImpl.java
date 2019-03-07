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
package org.apache.qpid.proton4j.engine.test.peer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.test.EngineTestDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HeaderHandlerImpl implements HeaderHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderHandlerImpl.class.getName());

    private final byte[] expectedHeader;
    private final byte[] response;
    private Runnable onCompletion;

    HeaderHandlerImpl(byte[] expectedHeader, byte[] response) {
        this(expectedHeader, response, null);
    }

    public HeaderHandlerImpl(byte[] header, byte[] response, Runnable onCompletion) {
        this.expectedHeader = header;
        this.response = response;
        this.onCompletion = onCompletion;
    }

    @Override
    public void header(byte[] header, EngineTestDriver peer) throws AssertionError {
        LOGGER.debug("About to check received header {}", new Binary(header));

        try {
            assertThat("Header should match", header, equalTo(expectedHeader));
        } catch (AssertionError ae) {
            LOGGER.error("Failure when verifying header", ae);
            peer.signalFailure(ae);
        }

        if (response == null || response.length == 0) {
            LOGGER.debug("Skipping header response as none was instructed");
        } else {
            LOGGER.debug("Sending header response: " + new Binary(response));
            peer.sendHeader(new AMQPHeader(response));
        }

        if (onCompletion != null) {
            onCompletion.run();
        } else {
            LOGGER.debug("No onCompletion action, doing nothing.");
        }
    }

    @Override
    public String toString() {
        return "HeaderHandlerImpl [_expectedHeader=" + new Binary(expectedHeader) + "]";
    }

    @Override
    public Runnable getOnCompletionAction() {
        return onCompletion;
    }

    @Override
    public Handler onCompletion(Runnable onCompletion) {
        this.onCompletion = onCompletion;
        return this;
    }
}