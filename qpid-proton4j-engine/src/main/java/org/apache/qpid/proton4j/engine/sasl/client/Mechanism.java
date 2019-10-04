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
package org.apache.qpid.proton4j.engine.sasl.client;

import javax.security.sasl.SaslException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Interface for all SASL authentication mechanism implementations.
 */
public interface Mechanism {

    /**
     * @return the well known name of this SASL mechanism.
     */
    Symbol getName();

    /**
     * Create an initial response based on selected mechanism.
     *
     * May be null if there is no initial response.
     *
     * @param credentials
     *      The credentials that are supplied for this SASL negotiation.
     *
     * @return the initial response, or null if there isn't one.
     *
     * @throws SaslException if an error occurs generating the initial response.
     */
    ProtonBuffer getInitialResponse(SaslCredentialsProvider credentials) throws SaslException;

    /**
     * Create a response based on a given challenge from the remote peer.
     *
     * @param credentials
     *      The credentials that are supplied for this SASL negotiation.
     * @param challenge
     *      The challenge that this Mechanism should response to.
     *
     * @return the response that answers the given challenge.
     *
     * @throws SaslException if an error occurs generating the challenge response.
     */
    ProtonBuffer getChallengeResponse(SaslCredentialsProvider credentials, ProtonBuffer challenge) throws SaslException;

    /**
     * Verifies that the SASL exchange has completed successfully. This is
     * an opportunity for the mechanism to ensure that all mandatory
     * steps have been completed successfully and to cleanup and resources
     * that are held by this Mechanism.
     *
     * @throws SaslException if the outcome of the SASL exchange is not valid for this Mechanism
     */
    void verifyCompletion() throws SaslException;

}
