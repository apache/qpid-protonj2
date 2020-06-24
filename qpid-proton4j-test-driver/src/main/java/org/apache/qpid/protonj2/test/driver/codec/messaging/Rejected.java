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
package org.apache.qpid.protonj2.test.driver.codec.messaging;

import java.util.List;

import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;

public class Rejected extends ListDescribedType implements DeliveryState, Outcome {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:rejected:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000025L);

    /**
     * Enumeration which maps to fields in the Rejected Performative
     */
    public enum Field {
        ERROR,
    }

    public Rejected() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Rejected(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Rejected(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public UnsignedLong getDescriptor() {
        return DESCRIPTOR_CODE;
    }

    public Rejected setError(ErrorCondition o) {
        getList().set(Field.ERROR.ordinal(), o);
        return this;
    }

    public ErrorCondition getError() {
        return (ErrorCondition) getList().get(Field.ERROR.ordinal());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof DescribedType)) {
            return false;
        }

        DescribedType d = (DescribedType) obj;
        if (!(DESCRIPTOR_CODE.equals(d.getDescriptor()) || DESCRIPTOR_SYMBOL.equals(d.getDescriptor()))) {
            return false;
        }

        Object described = getDescribed();
        Object described2 = d.getDescribed();
        if (described == null) {
            return described2 == null;
        } else {
            return described.equals(described2);
        }
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Rejected;
    }
}
