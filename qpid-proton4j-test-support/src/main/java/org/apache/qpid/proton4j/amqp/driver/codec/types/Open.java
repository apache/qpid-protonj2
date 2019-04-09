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

public class Open extends ListDescribedType {

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

    public Open setContainerId(Object o) {
        getList().set(Field.CONTAINER_ID.ordinal(), o);
        return this;
    }

    public Object getContainerId() {
        return getList().get(Field.CONTAINER_ID.ordinal());
    }

    public Open setHostname(Object o) {
        getList().set(Field.HOSTNAME.ordinal(), o);
        return this;
    }

    public Object getHostname() {
        return getList().get(Field.HOSTNAME.ordinal());
    }

    public Open setMaxFrameSize(Object o) {
        getList().set(Field.MAX_FRAME_SIZE.ordinal(), o);
        return this;
    }

    public Object getMaxFrameSize() {
        return getList().get(Field.MAX_FRAME_SIZE.ordinal());
    }

    public Open setChannelMax(Object o) {
        getList().set(Field.CHANNEL_MAX.ordinal(), o);
        return this;
    }

    public Object getChannelMax() {
        return getList().get(Field.CHANNEL_MAX.ordinal());
    }

    public Open setIdleTimeOut(Object o) {
        getList().set(Field.IDLE_TIME_OUT.ordinal(), o);
        return this;
    }

    public Object getIdleTimeOut() {
        return getList().get(Field.IDLE_TIME_OUT.ordinal());
    }

    public Open setOutgoingLocales(Object o) {
        getList().set(Field.OUTGOING_LOCALES.ordinal(), o);
        return this;
    }

    public Object getOutgoingLocales() {
        return getList().get(Field.OUTGOING_LOCALES.ordinal());
    }

    public Open setIncomingLocales(Object o) {
        getList().set(Field.INCOMING_LOCALES.ordinal(), o);
        return this;
    }

    public Object getIncomingLocales() {
        return getList().get(Field.INCOMING_LOCALES.ordinal());
    }

    public Open setOfferedCapabilities(Object o) {
        getList().set(Field.OFFERED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Object getOfferedCapabilities() {
        return getList().get(Field.OFFERED_CAPABILITIES.ordinal());
    }

    public Open setDesiredCapabilities(Object o) {
        getList().set(Field.DESIRED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Object getDesiredCapabilities() {
        return getList().get(Field.DESIRED_CAPABILITIES.ordinal());
    }

    public Open setProperties(Object o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    public Object getProperties() {
        return getList().get(Field.PROPERTIES.ordinal());
    }
}
