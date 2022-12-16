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

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Implements the SASL CRAM-MD5 authentication Mechanism.
 */
public class CramMD5Mechanism extends AbstractMechanism {

    /**
     * A singleton instance of the symbolic mechanism name.
     */
    public static final Symbol CRAM_MD5 = Symbol.valueOf("CRAM-MD5");

    private static final String ASCII = "ASCII";
    private static final String HMACMD5 = "HMACMD5";

    private boolean sentResponse;

    @Override
    public Symbol getName() {
        return CRAM_MD5;
    }

    @Override
    public boolean isApplicable(SaslCredentialsProvider credentials) {
        return credentials.username() != null && !credentials.username().isEmpty() &&
               credentials.password() != null && !credentials.password().isEmpty();
    }

    @Override
    public ProtonBuffer getInitialResponse(SaslCredentialsProvider credentials) {
        return null;
    }

    @Override
    public ProtonBuffer getChallengeResponse(SaslCredentialsProvider credentials, ProtonBuffer challenge) throws SaslException {
        if (!sentResponse && challenge != null && challenge.getReadableBytes() != 0) {
            try {
                SecretKeySpec key = new SecretKeySpec(credentials.password().getBytes(ASCII), HMACMD5);
                Mac mac = Mac.getInstance(HMACMD5);
                mac.init(key);

                byte[] challengeBytes = new byte[challenge.getReadableBytes()];

                challenge.readBytes(challengeBytes, 0, challengeBytes.length);

                byte[] bytes = mac.doFinal(challengeBytes);

                StringBuffer hash = new StringBuffer(credentials.username());
                hash.append(' ');
                for (int i = 0; i < bytes.length; i++) {
                    String hex = Integer.toHexString(0xFF & bytes[i]);
                    if (hex.length() == 1) {
                        hash.append('0');
                    }
                    hash.append(hex);
                }

                sentResponse = true;

                return ProtonBufferAllocator.defaultAllocator().copy(hash.toString().getBytes(ASCII)).convertToReadOnly();
            } catch (UnsupportedEncodingException e) {
                throw new SaslException("Unable to utilize required encoding", e);
            } catch (InvalidKeyException e) {
                throw new SaslException("Unable to utilize key", e);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException("Unable to utilize required algorithm", e);
            }
        } else {
            return EMPTY;
        }
    }

    @Override
    public void verifyCompletion() throws SaslException {
        super.verifyCompletion();

        if (!sentResponse) {
            throw new SaslException("SASL exchange was not fully completed.");
        }
    }
}
