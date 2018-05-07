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
package org.apache.qpid.proton4j.transport.sasl;

import org.apache.qpid.proton4j.amqp.Binary;

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
     * Called when a sasl-mechanisms frame has arrived and its effect
     * applied, indicating the offered mechanisms sent by the 'server' peer.
     *
     * @param context the SaslClientContext object
     */
    void onSaslMechanisms(SaslClientContext context, String[] mechanisms);

    /**
     * Called when a sasl-challenge frame has arrived and its effect
     * applied, indicating the challenge sent by the 'server' peer.
     *
     * @param context the SaslClientContext object
     * @param binary the challenge bytes sent from the sasl server.
     */
    void onSaslChallenge(SaslClientContext context, Binary binary);

    /**
     * Called when a sasl-outcome frame has arrived and its effect
     * applied, indicating the outcome and any success additional-data
     * sent by the 'server' peer.
     *
     * @param context the SaslClientContext object
     * @param additional the additional data sent from the server, or null if none.
     */
    void onSaslOutcome(SaslClientContext context, Binary additional);

}
