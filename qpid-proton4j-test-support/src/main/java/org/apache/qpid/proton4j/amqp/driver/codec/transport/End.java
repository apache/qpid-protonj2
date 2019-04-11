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
package org.apache.qpid.proton4j.amqp.driver.codec.transport;

import java.util.List;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public class End extends PerformativeDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:end:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000017L);

    /**
     * Enumeration which maps to fields in the End Performative
     */
    public enum Field {
        ERROR
    }

    public End() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public End(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public End(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public End setError(ErrorCondition o) {
        getList().set(Field.ERROR.ordinal(), o);
        return this;
    }

    public ErrorCondition getError() {
        return (ErrorCondition) getList().get(Field.ERROR.ordinal());
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.END;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleEnd(this, payload, channel, context);
    }
}
