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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Listener for SASL frame arrival to facilitate relevant handling for the SASL
 * negotiation of the server side of the SASL exchange.
 *
 * See the AMQP specification
 * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp51040">
 * SASL negotiation process</a> overview for related detail.
 */
public interface SaslServerListener {

    // TODO - Work out how to use ProtonBuffer instead of Binary

    /**
     * Called to give the application code a clear point to initialize all the Server side expectations.
     * <p>
     * The application should use this event to configure the server mechanisms and other server
     * authentication properties.
     *
     * @param context
     *      the {@link SaslServerContext} used to authenticate the connection.
     */
    default void initialize(SaslServerContext context) {}

    /**
     * Called when the SASL header has been received and the server is now ready to send the configured SASL
     * mechanisms.  The handler should respond be calling the {@link SaslServerContext#sendMechanisms(Symbol[])}
     * method immediately or later using the same thread that invoked this event handler.
     *
     * @param context
     *      the {@link SaslServerContext} object that received the SASL header.
     * @param header
     *      the AMQP Header that was read.
     *
     * @see SaslServerContext#sendMechanisms(Symbol[])
     */
    void handleSaslHeader(SaslServerContext context, AMQPHeader header);

    /**
     * Called when a SASL init frame has arrived from the client indicating the chosen
     * SASL mechanism and the initial response data if any.  Based on the chosen mechanism
     * the server handler should provide additional challenges or complete the SASL negotiation
     * by sending an outcome to the client.  The handler can either respond immediately or
     * it should response using the same thread that invoked this handler.
    *
     * @param context
     *      the {@link SaslServerContext} object that is to process the SASL initial frame.
     * @param mechanism
     *      the SASL mechanism that the client side has chosen for negotiations.
     * @param initResponse
     *      the initial response sent by the remote.
     *
     * @see SaslServerContext#sendChallenge(ProtonBuffer)
     * @see SaslServerContext#sendOutcome(SaslOutcome, ProtonBuffer)
     */
    void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse);

    /**
     * Called when a SASL response frame has arrived from the client.  The server should process
     * the response and either offer additional challenges or complete the SASL negotiations based
     * on the mechanics of the chosen SASL mechanism.  The server handler should either respond
     * immediately or should respond from the same thread that the response handler was invoked
     * from.
     *
     * @param context
     *      the {@link SaslServerContext} object that is to process the incoming response.
     * @param response
     *      the response sent by the remote SASL "client".
     *
     * @see SaslServerContext#sendChallenge(ProtonBuffer)
     * @see SaslServerContext#sendOutcome(SaslOutcome, ProtonBuffer)
     */
    void handleSaslResponse(SaslServerContext context, ProtonBuffer response);

}
