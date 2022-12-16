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
package org.apache.qpid.protonj2.test.driver.codec.transport;

import java.util.List;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;

import io.netty5.buffer.Buffer;

public class Detach extends PerformativeDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:detach:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000016L);

    /**
     * Enumeration which maps to fields in the Detach Performative
     */
    public enum Field {
        HANDLE,
        CLOSED,
        ERROR
    }

    public Detach() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Detach(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Detach(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public UnsignedLong getDescriptor() {
        return DESCRIPTOR_CODE;
    }

    public Detach setHandle(UnsignedInteger o) {
        getList().set(Field.HANDLE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getHandle() {
        return (UnsignedInteger) getList().get(Field.HANDLE.ordinal());
    }

    public Detach setClosed(Boolean o) {
        getList().set(Field.CLOSED.ordinal(), o);
        return this;
    }

    public Boolean getClosed() {
        return (Boolean) getList().get(Field.CLOSED.ordinal());
    }

    public Detach setError(ErrorCondition o) {
        getList().set(Field.ERROR.ordinal(), o);
        return this;
    }

    public Object getError() {
        return getList().get(Field.ERROR.ordinal());
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.DETACH;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, int frameSize, Buffer payload, int channel, E context) {
        handler.handleDetach(frameSize, this, payload, channel, context);
    }

    @Override
    public Object getFieldValueOrSpecDefault(int index) {
        Object result = getFieldValue(index);
        if (result == null) {
            Field field = Field.values()[index];
            switch (field) {
                case CLOSED:
                    result = Boolean.FALSE;
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "Detach{" +
               "handle=" + getHandle() +
               ", closed=" + getClosed() +
               ", error=" + getError() +
               '}';
    }
}
