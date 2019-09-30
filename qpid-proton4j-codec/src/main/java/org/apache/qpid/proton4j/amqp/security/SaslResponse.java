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

import java.util.Objects;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public final class SaslResponse implements SaslPerformative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000043L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-response:list");

    private ProtonBuffer response;

    public ProtonBuffer getResponse() {
        return response;
    }

    public SaslResponse setResponse(Binary response) {
        Objects.requireNonNull(response, "The response field is mandatory");
        setResponse(response.asProtonBuffer());
        return this;
    }

    public SaslResponse setResponse(ProtonBuffer response) {
        Objects.requireNonNull(response, "The response field is mandatory");
        this.response = response;
        return this;
    }

    @Override
    public String toString() {
        return "SaslResponse{" + "response=" + response + '}';
    }

    @Override
    public SaslResponse copy() {
        SaslResponse copy = new SaslResponse();
        if (response != null) {
            copy.setResponse(response.copy());
        }
        return copy;
    }

    @Override
    public SaslPerformativeType getPerformativeType() {
        return SaslPerformativeType.RESPONSE;
    }

    @Override
    public <E> void invoke(SaslPerformativeHandler<E> handler, E context) {
        handler.handleResponse(this, context);
    }
}
