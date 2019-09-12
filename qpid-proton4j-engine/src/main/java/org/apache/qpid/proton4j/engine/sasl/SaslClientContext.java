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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Engine;

/**
 * SASL Client operating context used by an {@link Engine} that has been
 * configured as a SASL client or that has initialed the SASL exchange by
 * being the first to initiate the AMQP header exchange.
 */
public interface SaslClientContext extends SaslContext {

    /**
     * @return the {@link SaslClientListener} that is currently assigned to this context.
     */
    SaslClientListener getClientListener();

    /**
     * Sets the {@link SaslClientListener} that will be called during SASL negotiations.
     *
     * @param listener
     *      The {@link SaslClientListener} instance to use for negotiation.
     *
     * @return this client context.
     */
    SaslClientContext setClientListener(SaslClientListener listener);

    /**
     * Sends a response to the SASL server indicating the chosen mechanism for this
     * client and the host name that this client is identifying itself as.
     *
     * @param mechanism
     *      The chosen mechanism selected from the list the server provided.
     * @param host
     *      The host name that the client is identified as.
     *
     * @return this client context.
     */
    SaslClientContext sendChosenMechanism(String mechanism, String host);

    /**
     * Sends a response to a server side challenge that comprises the challenge / response
     * exchange for the chosen SASL mechanism.
     *
     * @param response
     *      The response bytes to be sent to the server for this cycle.
     *
     * @return this client context.
     */
    SaslClientContext sendResponse(ProtonBuffer response);

}
