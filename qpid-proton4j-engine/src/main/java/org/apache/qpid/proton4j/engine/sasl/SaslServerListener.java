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

    /**
     * Called to give the application code a clear point to initialize all
     * the Server side expectations.
     * <p>
     * The application should use this event to configure the server mechanisms
     * and other server authentication properties.
     *
     * @param context
     *      the {@link SaslServerContext} used to authenticate the connection.
     */
    void initialize(SaslServerContext context);

    /**
     * Called when the sasl header has been received and the server
     * is now ready to send the configured sasl mechanisms.
     * <p>
     * The handler can use this callback to set the server's available
     * sasl mechanisms which will then be sent to the client.
     *
     * @param context
     *      the SaslServerContext object
     * @param header
     *      the AMQP Header that was read.
     */
    default void onSaslHeader(SaslServerContext context, AMQPHeader header) {}

    /**
     * Called when a sasl-init frame has arrived and its effect
     * applied, indicating the selected mechanism and any host name
     * and initial-response details from the 'client' peer.
     *
     * @param context
     *      the SaslServerContext object
     * @param initResponse
     *      the initial response sent by the remote
     */
    void onSaslInit(SaslServerContext context, ProtonBuffer initResponse);

    /**
     * Called when a sasl-response frame has arrived and its effect
     * applied, indicating the response sent by the 'client' peer.
     *
     * @param context
     *      the SaslServerContext object
     * @param response
     *      the response sent by the remote
     */
    void onSaslResponse(SaslServerContext context, ProtonBuffer response);

}
