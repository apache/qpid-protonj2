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
package org.apache.qpid.proton4j.test.driver.codec.transport;

import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedLong;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedShort;

import io.netty.buffer.ByteBuf;

public class Open extends PerformativeDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:open:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000010L);

    /**
     * Enumeration which maps to fields in the Open Performative
     */
    public enum Field {
        CONTAINER_ID,
        HOSTNAME,
        MAX_FRAME_SIZE,
        CHANNEL_MAX,
        IDLE_TIME_OUT,
        OUTGOING_LOCALES,
        INCOMING_LOCALES,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES,
    }

    public Open() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Open(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Open(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Open setContainerId(String o) {
        getList().set(Field.CONTAINER_ID.ordinal(), o);
        return this;
    }

    public String getContainerId() {
        return (String) getList().get(Field.CONTAINER_ID.ordinal());
    }

    public Open setHostname(String o) {
        getList().set(Field.HOSTNAME.ordinal(), o);
        return this;
    }

    public String getHostname() {
        return (String) getList().get(Field.HOSTNAME.ordinal());
    }

    public Open setMaxFrameSize(UnsignedInteger o) {
        getList().set(Field.MAX_FRAME_SIZE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getMaxFrameSize() {
        return (UnsignedInteger) getList().get(Field.MAX_FRAME_SIZE.ordinal());
    }

    public Open setChannelMax(UnsignedShort o) {
        getList().set(Field.CHANNEL_MAX.ordinal(), o);
        return this;
    }

    public UnsignedShort getChannelMax() {
        return (UnsignedShort) getList().get(Field.CHANNEL_MAX.ordinal());
    }

    public Open setIdleTimeOut(UnsignedInteger o) {
        getList().set(Field.IDLE_TIME_OUT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getIdleTimeOut() {
        return (UnsignedInteger) getList().get(Field.IDLE_TIME_OUT.ordinal());
    }

    public Open setOutgoingLocales(Symbol[] o) {
        getList().set(Field.OUTGOING_LOCALES.ordinal(), o);
        return this;
    }

    public Symbol[] getOutgoingLocales() {
        return (Symbol[]) getList().get(Field.OUTGOING_LOCALES.ordinal());
    }

    public Open setIncomingLocales(Symbol[] o) {
        getList().set(Field.INCOMING_LOCALES.ordinal(), o);
        return this;
    }

    public Symbol[] getIncomingLocales() {
        return (Symbol[]) getList().get(Field.INCOMING_LOCALES.ordinal());
    }

    public Open setOfferedCapabilities(Symbol[] o) {
        getList().set(Field.OFFERED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Symbol[] getOfferedCapabilities() {
        return (Symbol[]) getList().get(Field.OFFERED_CAPABILITIES.ordinal());
    }

    public Open setDesiredCapabilities(Symbol[] o) {
        getList().set(Field.DESIRED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Symbol[] getDesiredCapabilities() {
        return (Symbol[]) getList().get(Field.DESIRED_CAPABILITIES.ordinal());
    }

    public Open setProperties(Map<Symbol, Object> o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<Symbol, Object> getProperties() {
        return (Map<Symbol, Object>) getList().get(Field.PROPERTIES.ordinal());
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.OPEN;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ByteBuf payload, int channel, E context) {
        handler.handleOpen(this, payload, channel, context);
    }

    @Override
    public Object getFieldValueOrSpecDefault(int index) {
        Object result = getFieldValue(index);
        if (result == null) {
            Field field = Field.values()[index];
            switch (field) {
                case MAX_FRAME_SIZE:
                    result = UnsignedInteger.MAX_VALUE;
                    break;
                case CHANNEL_MAX:
                    result = UnsignedShort.MAX_VALUE;
                    break;
                default:
                    break;
            }
        }
        return result;
    }
}
