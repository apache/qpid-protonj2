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
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslStates;

public class SaslServerContext extends AbstractSaslContext {

    private final SaslServerListener listener;

    private boolean allowNonSasl;

    public SaslServerContext(SaslHandler handler, SaslServerListener listener) {
        super(handler);

        this.listener = listener;
    }

    @Override
    Role getRole() {
        return Role.SERVER;
    }

    /**
     * @return the SASL server listener.
     */
    public SaslServerListener getServerListener() {
        return listener;
    }

    //----- Remote Client state ----------------------------------------------//

    public String getClientMechanism() {
        return chosenMechanism.toString();
    }

    public String getClientHostname() {
        return hostname;
    }

    //----- Context mutable state --------------------------------------------//

    public String[] getMechanisms() {
        String[] mechanisms = null;

        if (serverMechanisms != null) {
            mechanisms = new String[serverMechanisms.length];
            for (int i = 0; i < serverMechanisms.length; i++) {
                mechanisms[i] = serverMechanisms[i].toString();
            }
        }

        return mechanisms;
    }

    public void setMechanisms(String[] mechanisms) {
        if (!mechanismsSent) {
            Symbol[] serverMechanisms = new Symbol[mechanisms.length];
            for (int i = 0; i < mechanisms.length; i++) {
                serverMechanisms[i] = Symbol.valueOf(mechanisms[i]);
            }

            this.serverMechanisms = serverMechanisms;
        } else {
            // TODO What is the right error here.
            throw new IllegalStateException("Server Mechanisms arlready sent to remote");
        }
    }

    /**
     * @return whether this Server allows non-sasl connection attempts
     */
    public boolean isAllowNonSasl() {
        return allowNonSasl;
    }

    /**
     * Determines if the server allows non-SASL connection attempts.
     *
     * @param allowNonSasl
     *      the configuration for allowing non-sasl connections
     */
    public void setAllowNonSasl(boolean allowNonSasl) {
        this.allowNonSasl = allowNonSasl;
    }

    //----- Transport event handlers -----------------------------------------//

    @Override
    public void handleAMQPHeader(TransportHandlerContext context, AMQPHeader header) {
        if (header.isSaslHeader()) {
            handleSaslHeader(context, header);
        } else {
            handleNonSaslHeader(context, header);
        }
    }

    private void handleSaslHeader(TransportHandlerContext context, AMQPHeader header) {
        if (!headerWritten) {
            context.fireWrite(AMQPHeader.getSASLHeader().getBuffer());
            headerWritten = true;
        }

        if (headerReceived) {
            // TODO - Error out on receive of another SASL Header.
            context.fireFailed(new IllegalStateException(
                "Unexpected second SASL Header read before SASL Authentication completed."));
        } else {
            headerReceived = true;
        }

        // TODO - When to fail when no mechanisms set, now or on some earlier started / connected evnet ?

        // TODO - Check state, then send mechanisms
        SaslMechanisms mechanisms = new SaslMechanisms();
        mechanisms.setSaslServerMechanisms(serverMechanisms);

        // Send the server mechanisms now.
        SaslFrame frame = new SaslFrame(mechanisms, null);
        context.fireWrite(frame);
        mechanismsSent = true;
        state = SaslStates.PN_SASL_STEP;
    }

    private void handleNonSaslHeader(TransportHandlerContext context, AMQPHeader header) {
        if (!headerReceived) {
            if (isAllowNonSasl()) {
                // Set proper outcome etc.
                done = true;
                context.fireAMQPHeader(header);
            } else {
                // TODO - Error type ?
                context.fireFailed(new IllegalStateException(
                    "Unexpected AMQP Header before SASL Authentication completed."));
            }
        } else {
            // Report the variety of errors that exist in this state such as the
            // fact that we shouldn't get an AMQP header before sasl is done, and when
            // it is done we are currently bypassing this call in the parent SaslHandler.
        }
    }

    @Override
    public void handleInit(SaslInit saslInit, Binary payload, TransportHandlerContext context) {
        hostname = saslInit.getHostname();
        chosenMechanism = saslInit.getMechanism();
        initReceived = true;

        if (saslInit.getInitialResponse() != null) {
            // TODO - How to present the response data, perhaps pass as arg to listener
            //        instead of storing pending bytes.
            //setPending(saslInit.getInitialResponse().asByteBuffer());
        }

        listener.onSaslInit(this);

        // TODO ? listener.onSaslInit(this, saslInit.getInitialResponse());
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, Binary payload, TransportHandlerContext context) {
        if (saslResponse.getResponse() != null) {
            // TODO - How to present the response data, perhaps pass as arg to listener
            //        instead of storing pending bytes.
            //setPending(saslResponse.getResponse().asByteBuffer());
        }

        listener.onSaslResponse(this);

        // TODO ? listener.onSaslResponse(this, saslResponse.getResponse());
    }
}
