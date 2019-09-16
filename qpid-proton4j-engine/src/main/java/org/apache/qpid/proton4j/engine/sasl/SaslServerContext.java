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
package org.apache.qpid.proton4j.engine.sasl;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.engine.Engine;

/**
 * SASL Server operating context used by an {@link Engine} that has been
 * configured as a SASL server or that has receiver an AMQP header thereby
 * forcing it into becoming the server side of the SASL exchange.
 */
public interface SaslServerContext extends SaslContext {

    /**
     * After the server has sent its supported mechanisms this method will return a
     * copy of that list for review by the server event handler.  If called before
     * the server has received the mechanisms list this method will return null.
     *
     * @return the mechanisms that the server offered to the client.
     */

    Symbol[] getServerMechanisms();

    /**
     * Returns the mechanism that was sent to the server to select the SASL mechanism
     * to use for negotiations.  If called before the client has sent its chosen mechanism
     * this method returns null.
     *
     * @return the SASL mechanism that the client selected to use for negotiation.
     */
    Symbol getChosenMechanism();

    /**
     * The DNS name of the host (either fully qualified or relative) that was sent to the server
     * which define the host the sending peer is connecting to.
     *
     * @return the host name the client has requested to connect to.
     */
    String getHostname();

    //----- SASL Negotiation API

    /**
     * Sends the set of supported mechanisms to the SASL client from which it must
     * choose and return one mechanism which will then be the basis for the SASL
     * authentication negotiation.
     *
     * @param mechanisms
     *      The mechanisms that this server supports.
     *
     * @return this server context.
     */
    SaslServerContext sendMechanisms(Symbol[] mechanisms);

    /**
     * Sends the SASL challenge defined by the SASL mechanism that is in use during
     * this SASL negotiation.  The challenge is an opaque binary that is provided to
     * the server by the security mechanism.
     *
     * @param challenge
     *      The buffer containing the server challenge.
     *
     * @return this server context.
     */
    SaslServerContext sendChallenge(Binary challenge);

    /**
     * Sends a response to a server side challenge that comprises the challenge / response
     * exchange for the chosen SASL mechanism.
     *
     * @param outcome
     *      The outcome of the SASL negotiation to be sent to the client.
     * @param additional
     *      The additional bytes to be sent from the server along with the outcome.
     *
     * @return this server context.
     */
    SaslServerContext sendOutcome(SaslOutcome outcome, Binary additional);

    //----- SASL Server Context event handlers

    /**
     * Called to give the application code a clear point to initialize all
     * the server side expectations.
     *
     * @param handler
     *      The handler that will handle initialization for the server context.
     */
    void initializationHandler(Consumer<SaslServerContext> handler);

    /**
     * Called when a SASL init frame has arrived from the client indicating the chosen
     * SASL mechanism and the initial response data if any.  Based on the chosen mechanism
     * the server handler should provide additional challenges or complete the SASL negotiation
     * by sending an outcome to the client.  The handler can either respond immediately or
     * it should response using the same thread that invoked this handler.
     *
     * @param handler
     *      The handler that will process the received SASL init from the client.
     */
    void saslInitHandler(BiConsumer<Symbol, Binary> handler);

    /**
     * Called when a SASL response frame has arrived from the client.  The server should process
     * the response and either offer additional challenges or complete the SASL negotiations based
     * on the mechanics of the chosen SASL mechanism.  The server handler should either respond
     * immediately or should respond from the same thread that the response handler was invoked
     * from.
     *
     * @param handler
     *      The handler that will process the received SASL response from the client.
     */
    void saslResponseHandler(Consumer<Binary> handler);

}
