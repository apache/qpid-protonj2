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
     * Sets the {@link SaslServerListener} that will be used to driver the server side SASL
     * negotiations with a connected "client".  As the client initiates or responds to the
     * various phases of the SASL negotiation the {@link SaslServerListener} will be notified
     * and allowed to respond.
     *
     * @param listener
     *      The {@link SaslServerListener} to use for SASL negotiations, cannot be null.
     *
     * @return this server context.
     */
    SaslServerContext setListener(SaslServerListener listener);

    /**
     * @return the currently set {@link SaslServerListener} instance.
     */
    SaslServerListener getListener();

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

}
