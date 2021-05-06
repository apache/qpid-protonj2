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
package org.apache.qpid.protonj2.types.security;

import java.util.Objects;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class SaslChallenge implements SaslPerformative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000042L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-challenge:list");

    private ProtonBuffer challenge;

    public ProtonBuffer getChallenge() {
        return challenge;
    }

    public SaslChallenge setChallenge(Binary challenge) {
        Objects.requireNonNull(challenge, "The challenge field is mandatory");
        setChallenge(challenge.asProtonBuffer());
        return this;
    }

    public SaslChallenge setChallenge(ProtonBuffer challenge) {
        Objects.requireNonNull(challenge, "The challenge field is mandatory");
        this.challenge = challenge;
        return this;
    }

    @Override
    public SaslChallenge copy() {
        SaslChallenge copy = new SaslChallenge();
        if (challenge != null) {
            copy.setChallenge(challenge.copy());
        }
        return copy;
    }

    @Override
    public String toString() {
        return "SaslChallenge{" + "challenge=" + (challenge == null ? null : StringUtils.toQuotedString(challenge)) + '}';
    }

    @Override
    public SaslPerformativeType getPerformativeType() {
        return SaslPerformativeType.CHALLENGE;
    }

    @Override
    public <E> void invoke(SaslPerformativeHandler<E> handler, E context) {
        handler.handleChallenge(this, context);
    }
}
