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
package org.apache.qpid.proton4j.amqp.security;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class SaslOutcome implements SaslPerformative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000044L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-outcome:list");

    private SaslCode code;
    private Binary additionalData;

    public SaslCode getCode() {
        return code;
    }

    public void setCode(SaslCode code) {
        if (code == null) {
            throw new NullPointerException("the code field is mandatory");
        }

        this.code = code;
    }

    public Binary getAdditionalData() {
        return additionalData;
    }

    public void setAdditionalData(Binary additionalData) {
        this.additionalData = additionalData;
    }

    @Override
    public SaslOutcome copy() {
        SaslOutcome copy = new SaslOutcome();

        copy.setCode(code);
        copy.setAdditionalData(additionalData == null ? null : additionalData.copy());

        return copy;
    }

    @Override
    public String toString() {
        return "SaslOutcome{" + "_code=" + code + ", _additionalData=" + additionalData + '}';
    }

    @Override
    public SaslPerformativeType getPerformativeType() {
        return SaslPerformativeType.OUTCOME;
    }

    @Override
    public <E> void invoke(SaslPerformativeHandler<E> handler, E context) {
        handler.handleOutcome(this, context);
    }
}
