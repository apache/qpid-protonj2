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
package org.apache.qpid.protonj2.engine.sasl.client;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Implements the SASL OAUTHBEARER authentication Mechanism .
 *
 * User name and Password values are sent without being encrypted.
 */
public class OauthBearerMechanism extends AbstractMechanism {

    // RFC 6750 Based pattern match, this protects against invalid tokens
    private static final Pattern OAUTH_BEARER_TOKEN_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-\\._~\\+/]+=*$");

    private static final byte SEPARATOR = 0x01;
    private static final byte[] USER_PREFIX = "n,a=".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] USER_SUFFIX = ",".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HOST_PREFIX = "host=".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] BEARER_PREFIX = "auth=Bearer ".getBytes(StandardCharsets.US_ASCII);

    /**
     * A singleton instance of the symbolic mechanism name.
     */
    public static final Symbol OAUTHBEARER = Symbol.valueOf("OAUTHBEARER");

    private String additionalFailureInformation;

    @Override
    public Symbol getName() {
        return OAUTHBEARER;
    }

    @Override
    public boolean isApplicable(SaslCredentialsProvider credentials) {
        // Enforce that user-name and password are required and that format for a bearer
        // token as defined in RFC 6750.
        if (credentials.isSecure() &&
            credentials.username() != null && !credentials.username().isEmpty() &&
            credentials.password() != null && !credentials.password().isEmpty()) {

            return OAUTH_BEARER_TOKEN_PATTERN.matcher(credentials.password()).matches();
        } else {
            return false;
        }
    }

    @Override
    public ProtonBuffer getInitialResponse(SaslCredentialsProvider credentials) throws SaslException {
        final byte[] username = credentials.username().getBytes(StandardCharsets.UTF_8);
        final byte[] token = credentials.password().getBytes(StandardCharsets.UTF_8);
        final byte[] vhost;

        if (credentials.vhost() != null && !credentials.vhost().isBlank()) {
            vhost = credentials.vhost().getBytes(StandardCharsets.US_ASCII);
        } else {
            vhost = null;
        }

        final int encodedSize;

        if (vhost != null) {
            encodedSize = USER_PREFIX.length + username.length + USER_SUFFIX.length + 1 +
                          HOST_PREFIX.length + vhost.length + 1 +
                          BEARER_PREFIX.length + token.length + 2;
        } else {
            encodedSize = USER_PREFIX.length + username.length + USER_SUFFIX.length + 1 +
                          BEARER_PREFIX.length + token.length + 2;
        }

        final ProtonBuffer response = ProtonBufferAllocator.defaultAllocator().allocate(encodedSize);

        response.writeBytes(USER_PREFIX);
        response.writeBytes(username);
        response.writeBytes(USER_SUFFIX);
        response.writeByte(SEPARATOR);

        if (vhost != null) {
            response.writeBytes(HOST_PREFIX);
            response.writeBytes(vhost);
            response.writeByte(SEPARATOR);
        }

        response.writeBytes(BEARER_PREFIX);
        response.writeBytes(token);
        response.writeByte(SEPARATOR);
        response.writeByte(SEPARATOR);

        return response;
    }

    @Override
    public ProtonBuffer getChallengeResponse(SaslCredentialsProvider credentials, ProtonBuffer challenge) throws SaslException {
        if (challenge != null && challenge.getReadableBytes() > 0 && additionalFailureInformation == null) {
            additionalFailureInformation = challenge.toString(StandardCharsets.UTF_8);
        }

        return EMPTY; // Empty response before remote sends SASL failed outcome
    }
}
