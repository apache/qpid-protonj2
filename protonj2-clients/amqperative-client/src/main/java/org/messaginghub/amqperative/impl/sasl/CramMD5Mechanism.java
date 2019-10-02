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

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

/**
 * Implements the SASL CRAM-MD5 authentication Mechanism.
 */
public class CramMD5Mechanism extends AbstractMechanism {

    public static final Symbol CRAM_MD5 = Symbol.valueOf("CRAM-MD5");

    private static final String ASCII = "ASCII";
    private static final String HMACMD5 = "HMACMD5";

    private boolean sentResponse;

    @Override
    public Symbol getName() {
        return CRAM_MD5;
    }

    @Override
    public ProtonBuffer getInitialResponse() {
        return EMPTY;
    }

    @Override
    public ProtonBuffer getChallengeResponse(ProtonBuffer challenge) {
        if (!sentResponse && challenge != null && challenge.getReadableBytes() != 0) {
            try {
                SecretKeySpec key = new SecretKeySpec(getPassword().getBytes(ASCII), HMACMD5);
                Mac mac = Mac.getInstance(HMACMD5);
                mac.init(key);

                byte[] challengeBytes = new byte[challenge.getReadableBytes()];

                challenge.readBytes(challengeBytes);

                byte[] bytes = mac.doFinal(challengeBytes);

                StringBuffer hash = new StringBuffer(getUsername());
                hash.append(' ');
                for (int i = 0; i < bytes.length; i++) {
                    String hex = Integer.toHexString(0xFF & bytes[i]);
                    if (hex.length() == 1) {
                        hash.append('0');
                    }
                    hash.append(hex);
                }

                sentResponse = true;

                return ProtonByteBufferAllocator.DEFAULT.wrap(hash.toString().getBytes(ASCII));

                // TODO - Need to define how we deal with error in mechanisms
            } catch (UnsupportedEncodingException e) {
                //throw new SaslException("Unable to utilise required encoding", e);
                throw new RuntimeException(e);
            } catch (InvalidKeyException e) {
                //throw new SaslException("Unable to utilise key", e);
                throw new RuntimeException(e);
            } catch (NoSuchAlgorithmException e) {
                //throw new SaslException("Unable to utilise required algorithm", e);
                throw new RuntimeException(e);
            }
        } else {
            return EMPTY;
        }
    }

    @Override
    public void verifyCompletion() {
        super.verifyCompletion();
        if (!sentResponse) {
            // TODO - Handle error from verify
            // throw new SaslException("SASL exchange was not fully completed.");
        }
    }
}
