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

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.Symbols;

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
     *
     * @throws EngineStateException if the engine has already shutdown or failed while processing the mechanisms.
     */
    SaslServerContext sendMechanisms(Symbol[] mechanisms) throws EngineStateException;

    /**
     * Sends the set of supported mechanisms to the SASL client from which it must
     * choose and return one mechanism which will then be the basis for the SASL
     * authentication negotiation.
     *
     * @param mechanisms
     *      The mechanisms that this server supports.
     *
     * @return this server context.
     *
     * @throws EngineStateException if the engine has already shutdown or failed while processing the mechanisms.
     */
    default SaslServerContext sendMechanisms(String[] mechanisms) throws EngineStateException {
        return sendMechanisms(Symbols.getSymbols(mechanisms));
    }

    /**
     * Sends the SASL challenge defined by the SASL mechanism that is in use during
     * this SASL negotiation.  The challenge is an opaque binary that is provided to
     * the server by the security mechanism.  Upon sending a challenge the SASL server
     * should expect a response from the SASL client.
     *
     * @param challenge
     *      The buffer containing the server challenge.
     *
     * @return this server context.
     *
     * @throws EngineStateException if the engine has already shutdown or failed while sending the challenge.
     */
    SaslServerContext sendChallenge(ProtonBuffer challenge) throws EngineStateException;

    /**
     * Sends a response to a server side challenge that comprises the challenge / response
     * exchange for the chosen SASL mechanism.
     * <p>
     * Sending an outcome that indicates SASL does not trigger a failure of the engine or
     * cause a shutdown, the caller must ensure that after failing the SASL exchange the
     * engine is shutdown or failed so that event handlers are triggered or should pro-actively
     * close any I/O channel that was opened. The {@link #saslFailure(SaslException)} method
     * has no effect once this method has been called as SASL negotiations are considered done
     * once an outcome is sent.
     *
     * @param outcome
     *      The outcome of the SASL negotiation to be sent to the client.
     * @param additional
     *      The additional bytes to be sent from the server along with the outcome.
     *
     * @return this server context.
     *
     * @throws EngineStateException if the engine has already shutdown or failed while processing the outcome.
     */
    SaslServerContext sendOutcome(SaslOutcome outcome, ProtonBuffer additional) throws EngineStateException;

    /**
     * Allows the server implementation to fail the SASL negotiation process due to some
     * unrecoverable error.  Failing the process will signal the {@link Engine} that the SASL process
     * has failed and place the engine in a failed state as well as notify the registered error
     * handler for the {@link Engine}.
     * <p>
     * This method will not perform any action if called after the {@link #sendOutcome(SaslOutcome, ProtonBuffer)}
     * method has been called as the SASL negotiation process was marked as completed and the implementation
     * is free to tear down SASL resources following that call.
     *
     * @param failure
     *      The exception to report to the {@link Engine} that describes the failure.
     *
     * @return this server context.
     */
    SaslServerContext saslFailure(SaslException failure);

}
