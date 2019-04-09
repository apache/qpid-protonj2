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

public class Attach extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:attach:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000012L);

    /**
     * Enumeration which maps to fields in the Attach Performative
     */
    public enum Field {
        NAME,
        HANDLE,
        ROLE,
        SND_SETTLE_MODE,
        RCV_SETTLE_MODE,
        SOURCE,
        TARGET,
        UNSETTLED,
        INCOMPLETE_UNSETTLED,
        INITIAL_DELIVERY_COUNT,
        MAX_MESSAGE_SIZE,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES
    }

    public Attach() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Attach(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Attach(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Attach setName(Object o) {
        getList().set(Field.NAME.ordinal(), o);
        return this;
    }

    public Object getName() {
        return getList().get(Field.NAME.ordinal());
    }

    public Attach setHandle(Object o) {
        getList().set(Field.HANDLE.ordinal(), o);
        return this;
    }

    public Object getHandle() {
        return getList().get(Field.HANDLE.ordinal());
    }

    public Attach setRole(Object o) {
        getList().set(Field.ROLE.ordinal(), o);
        return this;
    }

    public Object getRole() {
        return getList().get(Field.ROLE.ordinal());
    }

    public Attach setSndSettleMode(Object o) {
        getList().set(Field.SND_SETTLE_MODE.ordinal(), o);
        return this;
    }

    public Object getSndSettleMode() {
        return getList().get(Field.SND_SETTLE_MODE.ordinal());
    }

    public Attach setRcvSettleMode(Object o) {
        getList().set(Field.RCV_SETTLE_MODE.ordinal(), o);
        return this;
    }

    public Object getRcvSettleMode() {
        return getList().get(Field.RCV_SETTLE_MODE.ordinal());
    }

    public Attach setSource(Object o) {
        getList().set(Field.SOURCE.ordinal(), o);
        return this;
    }

    public Object getSource() {
        return getList().get(Field.SOURCE.ordinal());
    }

    public Attach setTarget(Object o) {
        getList().set(Field.TARGET.ordinal(), o);
        return this;
    }

    public Object getTarget() {
        return getList().get(Field.TARGET.ordinal());
    }

    public Attach setUnsettled(Object o) {
        getList().set(Field.UNSETTLED.ordinal(), o);
        return this;
    }

    public Object getUnsettled() {
        return getList().get(Field.UNSETTLED.ordinal());
    }

    public Attach setIncompleteUnsettled(Object o) {
        getList().set(Field.INCOMPLETE_UNSETTLED.ordinal(), o);
        return this;
    }

    public Object getIncompleteUnsettled() {
        return getList().get(Field.INCOMPLETE_UNSETTLED.ordinal());
    }

    public Attach setInitialDeliveryCount(Object o) {
        getList().set(Field.INITIAL_DELIVERY_COUNT.ordinal(), o);
        return this;
    }

    public Object getInitialDeliveryCount() {
        return getList().get(Field.INITIAL_DELIVERY_COUNT.ordinal());
    }

    public Attach setMaxMessageSize(Object o) {
        getList().set(Field.MAX_MESSAGE_SIZE.ordinal(), o);
        return this;
    }

    public Object getMaxMessageSize() {
        return getList().get(Field.MAX_MESSAGE_SIZE.ordinal());
    }

    public Attach setOfferedCapabilities(Object o) {
        getList().set(Field.OFFERED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Object getOfferedCapabilities() {
        return getList().get(Field.OFFERED_CAPABILITIES.ordinal());
    }

    public Attach setDesiredCapabilities(Object o) {
        getList().set(Field.DESIRED_CAPABILITIES.ordinal(), o);
        return this;
    }

    public Object getDesiredCapabilities() {
        return getList().get(Field.DESIRED_CAPABILITIES.ordinal());
    }

    public Attach setProperties(Object o) {
        getList().set(Field.PROPERTIES.ordinal(), o);
        return this;
    }

    public Object getProperties() {
        return getList().get(Field.PROPERTIES.ordinal());
    }
}
