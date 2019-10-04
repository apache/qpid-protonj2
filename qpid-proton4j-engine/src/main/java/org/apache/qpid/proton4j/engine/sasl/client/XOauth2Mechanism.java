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

import java.nio.charset.StandardCharsets;

import javax.security.sasl.SaslException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

/**
 * Implements the SASL XOAUTH2 authentication Mechanism .
 *
 * User name and Password values are sent without being encrypted.
 */
public class XOauth2Mechanism extends AbstractMechanism {

    public static final Symbol XOAUTH2 = Symbol.valueOf("XOAUTH2");

    private String additionalFailureInformation;

    @Override
    public Symbol getName() {
        return XOAUTH2;
    }

    @Override
    public ProtonBuffer getInitialResponse(SaslCredentialsProvider credentials) throws SaslException {

        String username = credentials.username();
        String password = credentials.password();

        if (username == null) {
            username = "";
        }

        if (password == null) {
            password = "";
        }

        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[usernameBytes.length + passwordBytes.length + 20];
        System.arraycopy("user=".getBytes(StandardCharsets.US_ASCII), 0, data, 0, 5);
        System.arraycopy(usernameBytes, 0, data, 5, usernameBytes.length);
        data[5+usernameBytes.length] = 1;
        System.arraycopy("auth=Bearer ".getBytes(StandardCharsets.US_ASCII), 0, data, 6+usernameBytes.length, 12);
        System.arraycopy(passwordBytes, 0, data, 18 + usernameBytes.length, passwordBytes.length);
        data[data.length-2] = 1;
        data[data.length-1] = 1;

        return ProtonByteBufferAllocator.DEFAULT.wrap(data).setWriteIndex(data.length);
    }

    @Override
    public ProtonBuffer getChallengeResponse(SaslCredentialsProvider credentials, ProtonBuffer challenge) throws SaslException {
        if (challenge != null && challenge.getReadableBytes() > 0 && additionalFailureInformation == null) {
            additionalFailureInformation = challenge.toString(StandardCharsets.UTF_8);
        }

        return EMPTY;
    }
}