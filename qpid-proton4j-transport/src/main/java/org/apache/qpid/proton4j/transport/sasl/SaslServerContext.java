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

public class SaslServerContext extends AbstractSaslContext {

    private final SaslServerListener listener;

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
        return null;
    }

    public String getClientHostname() {
        return null;
    }

    // TODO - Mutable state

    public String[] getMechanisms() {
        return null;
    }

    public void setMechanisms(String[] mechanisms) {

    }
}
