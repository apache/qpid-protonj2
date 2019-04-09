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

public class Flow extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:flow:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000013L);

    /**
     * Enumeration which maps to fields in the Flow Performative
     */
    public enum Field {
        NEXT_INCOMING_ID,
        INCOMING_WINDOW,
        NEXT_OUTGOING_ID,
        OUTGOING_WINDOW,
        HANDLE,
        DELIVERY_COUNT,
        LINK_CREDIT,
        AVAILABLE,
        DRAIN,
        ECHO,
        PROPERTIES,
    }

    public Flow() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Flow(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Flow(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Flow setNextIncomingId(Object o) {
        getList().set(Field.NEXT_INCOMING_ID.ordinal(), o);
        return this;
    }

    public Object getNextIncomingId() {
        return getList().get(Field.NEXT_INCOMING_ID.ordinal());
    }

    public Flow setIncomingWindow(Object o) {
        getList().set(Field.INCOMING_WINDOW.ordinal(), o);
        return this;
    }

    public Object getIncomingWindow() {
        return getList().get(Field.INCOMING_WINDOW.ordinal());
    }

    public Flow setNextOutgoingId(Object o) {
        getList().set(Field.NEXT_OUTGOING_ID.ordinal(), o);
        return this;
    }

    public Object getNextOutgoingId() {
        return getList().get(Field.NEXT_OUTGOING_ID.ordinal());
    }

    public Flow setOutgoingWindow(Object o) {
        getList().set(Field.OUTGOING_WINDOW.ordinal(), o);
        return this;
    }

    public Object getOutgoingWindow() {
        return getList().get(Field.OUTGOING_WINDOW.ordinal());
    }

    public Flow setHandle(Object o) {
        getList().set(Field.HANDLE.ordinal(), o);
        return this;
    }

    public Object getHandle() {
        return getList().get(Field.HANDLE.ordinal());
    }

    public Flow setDeliveryCount(Object o) {
        getList().set(Field.DELIVERY_COUNT.ordinal(), o);
        return this;
    }

    public Object getDeliveryCount() {
        return getList().get(Field.DELIVERY_COUNT.ordinal());
    }

    public Flow setLinkCredit(Object o) {
        getList().set(Field.LINK_CREDIT.ordinal(), o);
        return this;
    }

    public Object getLinkCredit() {
        return getList().get(Field.LINK_CREDIT.ordinal());
    }

    public Flow setAvailable(Object o) {
        getList().set(Field.AVAILABLE.ordinal(), o);
        return this;
    }

    public Object getAvailable() {
        return getList().get(Field.AVAILABLE.ordinal());
    }

    public Flow setDrain(Object o) {
        getList().set(Field.DRAIN.ordinal(), o);
        return this;
    }

    public Object getDrain() {
        return getList().get(Field.DRAIN.ordinal());
    }

    public Flow setEcho(Object o) {
        getList().set(Field.ECHO.ordinal(), o);
        return this;
    }

    public Object getEcho() {
        return getList().get(Field.ECHO.ordinal());
    }

    public Flow setProperties(Object o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    public Object getProperties() {
        return getList().get(Field.PROPERTIES.ordinal());
    }
}
