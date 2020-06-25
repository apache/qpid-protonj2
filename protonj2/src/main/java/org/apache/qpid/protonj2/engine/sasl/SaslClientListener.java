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
package org.apache.qpid.protonj2.engine.sasl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Listener for SASL frame arrival to facilitate relevant handling for the SASL
 * negotiation of the client side of the SASL exchange.
 *
 * See the AMQP specification
 * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp51040">
 * SASL negotiation process</a> overview for related detail.
 */
public interface SaslClientListener {

    /**
     * Called to give the application code a clear point to initialize all the client side expectations.
     * <p>
     * The application should use this event to configure the client mechanisms and other client
     * authentication properties.
     * <p>
     * In the event that the client implementation cannot proceed with SASL authentication it should call the
     * {@link SaslClientContext#saslFailure(org.apache.qpid.protonj2.engine.exceptions.SaslException)}
     * to signal the {@link Engine} that it should transition to a failed state.
     *
     * @param context
     *      the {@link SaslClientContext} used to authenticate the connection.
     */
    default void initialize(SaslClientContext context) {}

    /**
     * Called when a SASL mechanisms frame has arrived and its effect applied, indicating
     * the offered mechanisms sent by the 'server' peer.  The client should respond to the
     * mechanisms event by selecting one from the offered list and calling the
     * {@link SaslClientContext#sendChosenMechanism(Symbol, String, ProtonBuffer)} method immediately
     * or later using the same thread that triggered this event.
     * <p>
     * In the event that the client implementation cannot proceed with SASL authentication it should call the
     * {@link SaslClientContext#saslFailure(org.apache.qpid.protonj2.engine.exceptions.SaslException)} to fail
     * the SASL negotiation and signal the {@link Engine} that it should transition to a failed state.
     *
     * @param context
     *      the {@link SaslClientContext} that is to handle the mechanism selection
     * @param mechanisms
     *      the mechanisms that the remote supports.
     *
     * @see SaslClientContext#sendChosenMechanism(Symbol, String, ProtonBuffer)
     * @see SaslClientContext#saslFailure(javax.security.sasl.SaslException)
     */
    void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms);

    /**
     * Called when a SASL challenge frame has arrived and its effect applied, indicating the
     * challenge sent by the 'server' peer.  The client should respond to the mechanisms event
     *  by selecting one from the offered list and calling the
     * {@link SaslClientContext#sendResponse(ProtonBuffer)} method immediately or later using the same
     * thread that triggered this event.
     * <p>
     * In the event that the client implementation cannot proceed with SASL authentication it should call the
     * {@link SaslClientContext#saslFailure(org.apache.qpid.protonj2.engine.exceptions.SaslException)} to fail
     * the SASL negotiation and signal the {@link Engine} that it should transition to a failed state.
     *
     * @param context
     *      the {@link SaslClientContext} that is to handle the SASL challenge.
     * @param challenge
     *      the challenge bytes sent from the SASL server.
     *
     * @see SaslClientContext#sendResponse(ProtonBuffer)
     * @see SaslClientContext#saslFailure(javax.security.sasl.SaslException)
     */
    void handleSaslChallenge(SaslClientContext context, ProtonBuffer challenge);

    /**
     * Called when a SASL outcome frame has arrived and its effect applied, indicating the outcome and
     * any success additional data sent by the 'server' peer.  The client can consider the SASL negotiations
     * complete following this event.  The client should respond appropriately to the outcome whose state can
     * indicate that negotiations have failed and the server has not authenticated the client.
     * <p>
     * In the event that the client implementation cannot proceed with SASL authentication it should call the
     * {@link SaslClientContext#saslFailure(org.apache.qpid.protonj2.engine.exceptions.SaslException)} to fail
     * the SASL negotiation and signal the {@link Engine} that it should transition to a failed state.
     *
     * @param context
     *      the {@link SaslClientContext} that is to handle the resulting SASL outcome.
     * @param outcome
     *      the outcome that was supplied by the SASL "server".
     * @param additional
     *      the additional data sent from the server, or null if none.
     *
     * @see SaslClientContext#saslFailure(javax.security.sasl.SaslException)
     */
    void handleSaslOutcome(SaslClientContext context, SaslOutcome outcome, ProtonBuffer additional);

}
