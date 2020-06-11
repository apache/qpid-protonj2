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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Symbol;

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

    /**
     * Allows the Mechanism to determine if it is a valid choice based on the configured
     * credentials at the time of selection.
     *
     * @param credentials
     *      the login credentials available at the time of mechanism selection.
     *
     * @return true if the mechanism can be used with the provided credentials
     */
    boolean isApplicable(SaslCredentialsProvider credentials);

    /**
     * Allows the mechanism to indicate if it is enabled by default, or only when explicitly enabled
     * through configuring the permitted SASL mechanisms.  Any mechanism selection logic should examine
     * this value along with the configured allowed mechanism and decide if this one should be used.
     *
     * Typically most mechanisms can be enabled by default but some require explicit configuration
     * in order to operate which implies that selecting them by default would always cause an authentication
     * error if that mechanism matches the highest priority value offered by the remote peer.
     *
     * @return true if this Mechanism is enabled by default.
     */
    public boolean isEnabledByDefault();

}
