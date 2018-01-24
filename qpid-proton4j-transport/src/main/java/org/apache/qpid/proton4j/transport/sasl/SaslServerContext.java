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

import org.apache.qpid.proton4j.amqp.Symbol;

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

    // TODO - Client state

    public String getClientMechanism() {
        return chosenMechanism.toString();
    }

    public String getClientHostname() {
        return hostname;
    }

    // TODO - Mutable state

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
}
