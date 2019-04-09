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
package org.apache.qpid.proton4j.amqp.driver.codec.types;

import java.util.List;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class Begin extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:begin:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000011L);

    /**
     * Enumeration which maps to fields in the Begin Performative
     */
    public enum Field {
        REMOTE_CHANNEL,
        NEXT_OUTGOING_ID,
        INCOMING_WINDOW,
        OUTGOING_WINDOW,
        HANDLE_MAX,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES,
    }

    public Begin() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Begin(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Begin(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Begin setRemoteChannel(Object o) {
        getList().set(Field.REMOTE_CHANNEL.ordinal(), o);
        return this;
    }

    public Object getRemoteChannel() {
        return getList().get(Field.REMOTE_CHANNEL.ordinal());
    }

    public Begin setNextOutgoingId(Object o) {
        getList().set(Field.NEXT_OUTGOING_ID.ordinal(), o);
        return this;
    }

    public Object getNextOutgoingId() {
        return getList().get(Field.NEXT_OUTGOING_ID.ordinal());
    }

    public Begin setIncomingWindow(Object o) {
        getList().set(Field.INCOMING_WINDOW.ordinal(), o);
        return this;
    }

    public Object getIncomingWindow() {
        return getList().get(Field.INCOMING_WINDOW.ordinal());
    }

    public Begin setOutgoingWindow(Object o) {
        getList().set(Field.OUTGOING_WINDOW.ordinal(), o);
        return this;
    }

    public Object getOutgoingWindow() {
        return getList().get(Field.OUTGOING_WINDOW.ordinal());
    }

    public Begin setHandleMax(Object o) {
        getList().set(Field.HANDLE_MAX.ordinal(), o);
        return this;
    }

    public Object getHandleMax() {
        return getList().get(Field.HANDLE_MAX.ordinal());
    }

    public Begin setOfferedCapabilities(Object o) {
        getList().set(Field.OFFERED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Object getOfferedCapabilities() {
        return getList().get(Field.OFFERED_CAPABILITIES.ordinal());
    }

    public Begin setDesiredCapabilities(Object o) {
        getList().set(Field.DESIRED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Object getDesiredCapabilities() {
        return getList().get(Field.DESIRED_CAPABILITIES.ordinal());
    }

    public Begin setProperties(Object o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    public Object getProperties() {
        return getList().get(Field.PROPERTIES.ordinal());
    }
}
