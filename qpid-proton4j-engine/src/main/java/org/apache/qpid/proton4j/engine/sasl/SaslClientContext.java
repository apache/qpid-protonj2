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

import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;

/**
 * SASL Client operating context used by an {@link Engine} that has been
 * configured as a SASL client or that has initialed the SASL exchange by
 * being the first to initiate the AMQP header exchange.
 */
public interface SaslClientContext extends SaslContext {

    /**
     * After the server has sent its supported mechanisms this method will return
     * a copy of that list for review by the client.  If called before the server
     * has sent its mechanisms list this method will return null.
     *
     * @return the mechanisms that the server offered to the client.
     */
    @Override
    Symbol[] getServerMechanisms();

    /**
     * Returns the mechanism that was sent to the server to select the SASL mechanism
     * to use for negotiations.  If called before the client has sent its chosen mechanism
     * this method returns null.
     *
     * @return the SASL mechanism that the client selected to use for negotiation.
     */
    @Override
    Symbol getChosenMechanism();

    /**
     * The DNS name of the host (either fully qualified or relative) that was sent to the server
     * which define the host the sending peer is connecting to.
     *
     * @return the host name the client has requested to connect to.
     */
    @Override
    String getHostname();

    /**
     * Sets the {@link SaslClientListener} that will be used to driver the client side SASL
     * negotiations with a connected "server".  As the server initiates or responds to the
     * various phases of the SASL negotiation the {@link SaslClientListener} will be notified
     * and allowed to respond.
     *
     * @param listener
     *      The {@link SaslClientListener} to use for SASL negotiations, cannot be null.
     *
     * @return this client context.
     */
    SaslClientContext setListener(SaslClientListener listener);

    /**
     * @return the currently set {@link SaslClientListener} instance.
     */
    SaslClientListener getListener();

    //----- SASL Negotiation API

    /**
     * Sends the AMQP Header indicating the desire for SASL negotiations to be commenced on
     * this connection.  The hosting application my wish to start SASL negotiations prior to
     * opening a {@link Connection} in order to validation authentication state out of band
     * of the normal open process.
     *
     * @return this client context.
     */
    SaslClientContext sendSASLHeader();

    /**
     * Sends a response to the SASL server indicating the chosen mechanism for this
     * client and the host name that this client is identifying itself as.
     *
     * @param mechanism
     *      The chosen mechanism selected from the list the server provided.
     * @param host
     *      The host name that the client is identified as or null if none selected.
     * @param initialResponse
     *      The initial response data sent as defined by the chosen mechanism or null if none required.
     *
     * @return this client context.
     */
    SaslClientContext sendChosenMechanism(Symbol mechanism, String host, Binary initialResponse);

    /**
     * Sends a response to a server side challenge that comprises the challenge / response
     * exchange for the chosen SASL mechanism.
     *
     * @param response
     *      The response bytes to be sent to the server for this cycle.
     *
     * @return this client context.
     */
    SaslClientContext sendResponse(Binary response);

    //----- SASL Client Context event handlers

    /**
     * Called to give the application code a clear point to initialize all
     * the client side expectations.
     *
     * @param context
     *      the {@link SaslClientContext} used to authenticate the connection.
     */
    void initializationHandler(Consumer<SaslClientContext> context);

    /**
     * Called when a SASL mechanisms frame has arrived and the client should choose which
     * mechanism to select for SASL negotiations.  The client should either respond immediately
     * via the {@link #sendChosenMechanism(Symbol, String, Binary)} method or do so later from the same
     * thread upon which this handler was invoked.
     *
     * @param handler
     *      The handler that will process the received SASL mechanisms from the server.
     */
    void saslMechanismsHandler(Consumer<Symbol[]> handler);

    /**
     * Called when a SASL challenge frame has arrived and the client should provide a response
     * to the challenge based on the chosen SASL mechanism in use.  The client should either respond
     * immediately via the {@link #sendResponse(Binary)} method or do so later from the same thread
     * upon which this handler was invoked.
     *
     * @param handler
     *      The handler that will process and respond to SASL challenges from the server.
     */
    void saslChallengeHandler(Consumer<Binary> handler);

    /**
     * Called when a SASL outcome frame has arrived to indicate the result of the SASL negotiation
     * with the server instance.  For a successful outcome the client need not react in any way. If
     * the outcome is a failure then the client may react by closing down the current connection.
     *
     * @param handler
     *      The handler that will process and react to the outcome of the SASL negotiation.
     */
    void saslOutcomeHandler(Consumer<Binary> handler);

}
