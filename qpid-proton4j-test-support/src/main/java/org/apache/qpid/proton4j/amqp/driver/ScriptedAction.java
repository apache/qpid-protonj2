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
package org.apache.qpid.proton4j.amqp.driver;

import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslInit;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Attach;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Begin;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Close;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Detach;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Disposition;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.End;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Flow;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Open;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Transfer;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Entry in the test script that produces some output to be sent to the AMQP
 * peer under test.
 */
public interface ScriptedAction extends ScriptedElement {

    @Override
    default ScriptEntryType getType() {
        return ScriptEntryType.ACTION;
    }

    /**
     * Runs the scripted action on its associated test driver immediately
     * regardless of any queued tasks or expected inputs.
     */
    void now();

    /**
     * Queues the scripted action for later run after any preceding scripted
     * elements are performed.
     */
    void queue();

    /**
     * Triggers the action to be performed on the given {@link Consumer}.
     *
     * @param driver
     *      The test driver that is managing the test
     */
    void perform(AMQPTestDriver driver);

    // By default the Action type is not expecting to be triggered by an incoming
    // AMQP frame so in all these cases we fail because the script was wrong or the
    // remote sent something we didn't expect.

    @Override
    default void handleAMQPHeader(AMQPHeader header, AMQPTestDriver context) {
        throw new AssertionError("AMQP Header arrived when expecting to perform an action");
    }

    @Override
    default void handleSASLHeader(AMQPHeader header, AMQPTestDriver context) {
        throw new AssertionError("SASL Header arrived when expecting to perform an action");
    }

    @Override
    default void handleOpen(Open open, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Open arrived when expecting to perform an action");
    }

    @Override
    default void handleBegin(Begin begin, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Begin arrived when expecting to perform an action");
    }

    @Override
    default void handleAttach(Attach attach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Attach arrived when expecting to perform an action");
    }

    @Override
    default void handleFlow(Flow flow, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Flow arrived when expecting to perform an action");
    }

    @Override
    default void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Transfer arrived when expecting to perform an action");
    }

    @Override
    default void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Disposition arrived when expecting to perform an action");
    }

    @Override
    default void handleDetach(Detach detach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Detach arrived when expecting to perform an action");
    }

    @Override
    default void handleEnd(End end, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("End arrived when expecting to perform an action");
    }

    @Override
    default void handleClose(Close close, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        throw new AssertionError("Close arrived when expecting to perform an action");
    }

    @Override
    default void handleMechanisms(SaslMechanisms saslMechanisms, AMQPTestDriver context) {
        throw new AssertionError("SaslMechanisms arrived when expecting to perform an action");
    }

    @Override
    default void handleInit(SaslInit saslInit, AMQPTestDriver context) {
        throw new AssertionError("SaslInit arrived when expecting to perform an action");
    }

    @Override
    default void handleChallenge(SaslChallenge saslChallenge, AMQPTestDriver context) {
        throw new AssertionError("SaslChallenge arrived when expecting to perform an action");
    }

    @Override
    default void handleResponse(SaslResponse saslResponse, AMQPTestDriver context) {
        throw new AssertionError("SaslResponse arrived when expecting to perform an action");
    }

    @Override
    default void handleOutcome(SaslOutcome saslOutcome, AMQPTestDriver context) {
        throw new AssertionError("SaslOutcome arrived when expecting to perform an action");
    }
}
