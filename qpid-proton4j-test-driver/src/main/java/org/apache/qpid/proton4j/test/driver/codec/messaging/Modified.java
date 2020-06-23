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
package org.apache.qpid.proton4j.test.driver.codec.messaging;

import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.proton4j.test.driver.codec.transport.DeliveryState;

public class Modified extends ListDescribedType implements DeliveryState, Outcome {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:modified:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000027L);

    /**
     * Enumeration which maps to fields in the Modified Performative
     */
    public enum Field {
        DELIVERY_FAILED,
        UNDELIVERABLE_HERE,
        MESSAGE_ANNOTATIONS
    }

    public Modified() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Modified(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Modified(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Modified setDeliveryFailed(Boolean o) {
        getList().set(Field.DELIVERY_FAILED.ordinal(), o);
        return this;
    }

    public Boolean getDeliveryFailed() {
        return (Boolean) getList().get(Field.DELIVERY_FAILED.ordinal());
    }

    public Modified setUndeliverableHere(Boolean o) {
        getList().set(Field.UNDELIVERABLE_HERE.ordinal(), o);
        return this;
    }

    public Boolean getUndeliverableHere() {
        return (Boolean) getList().get(Field.UNDELIVERABLE_HERE.ordinal());
    }

    public Modified setMessageAnnotations(Map<Symbol, Object> o) {
        getList().set(Field.MESSAGE_ANNOTATIONS.ordinal(), o);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<Symbol, Object> getMessageAnnotations() {
        return (Map<Symbol, Object>) getList().get(Field.MESSAGE_ANNOTATIONS.ordinal());
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
        return DeliveryStateType.Modified;
    }
}
