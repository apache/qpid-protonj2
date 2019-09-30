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
package org.messaginghub.amqperative.impl.sasl;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslClientListener;
import org.apache.qpid.proton4j.engine.sasl.SaslOutcome;
import org.messaginghub.amqperative.impl.ClientConnection;

/**
 * Handles SASL traffic from the proton engine and drives the authentication process
 */
public class SaslAuthenticator implements SaslClientListener {

    private final ClientConnection connection;
    private final SaslMechanismSelector mechanisms;

    public SaslAuthenticator(ClientConnection connection, SaslMechanismSelector mechanisms) {
        this.connection = connection;
        this.mechanisms = mechanisms;
    }

    @Override
    public void handleSaslMechanisms(SaslClientContext context, Symbol[] mechanisms) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleSaslChallenge(SaslClientContext context, ProtonBuffer challenge) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleSaslOutcome(SaslClientContext context, SaslOutcome outcome, ProtonBuffer additional) {
        // TODO Auto-generated method stub

    }
}
