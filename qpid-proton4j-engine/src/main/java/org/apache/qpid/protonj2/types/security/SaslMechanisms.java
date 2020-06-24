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

import java.util.Arrays;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class SaslMechanisms implements SaslPerformative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000040L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:sasl-mechanisms:list");

    private Symbol[] saslServerMechanisms;

    public Symbol[] getSaslServerMechanisms() {
        return saslServerMechanisms;
    }

    public SaslMechanisms setSaslServerMechanisms(Symbol... saslServerMechanisms) {
        if (saslServerMechanisms == null) {
            throw new NullPointerException("the sasl-server-mechanisms field is mandatory");
        }

        this.saslServerMechanisms = saslServerMechanisms;
        return this;
    }

    @Override
    public SaslMechanisms copy() {
        SaslMechanisms copy = new SaslMechanisms();
        if (saslServerMechanisms != null) {
            copy.setSaslServerMechanisms(Arrays.copyOf(saslServerMechanisms, saslServerMechanisms.length));
        }
        return copy;
    }

    @Override
    public String toString() {
        return "SaslMechanisms{" + "saslServerMechanisms=" +
                    (saslServerMechanisms == null ? null : Arrays.asList(saslServerMechanisms)) + '}';
    }

    @Override
    public SaslPerformativeType getPerformativeType() {
        return SaslPerformativeType.MECHANISMS;
    }

    @Override
    public <E> void invoke(SaslPerformativeHandler<E> handler, E context) {
        handler.handleMechanisms(this, context);
    }
}
