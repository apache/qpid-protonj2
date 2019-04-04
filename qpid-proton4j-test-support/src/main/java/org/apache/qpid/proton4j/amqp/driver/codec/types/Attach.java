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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class Attach extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:attach:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000012L);

    private static final int FIELD_NAME = 0;
    private static final int FIELD_HANDLE = 1;
    private static final int FIELD_ROLE = 2;
    private static final int FIELD_SND_SETTLE_MODE = 3;
    private static final int FIELD_RCV_SETTLE_MODE = 4;
    private static final int FIELD_SOURCE = 5;
    private static final int FIELD_TARGET = 6;
    private static final int FIELD_UNSETTLED = 7;
    private static final int FIELD_INCOMPLETE_UNSETTLED = 8;
    private static final int FIELD_INITIAL_DELIVERY_COUNT = 9;
    private static final int FIELD_MAX_MESSAGE_SIZE = 10;
    private static final int FIELD_OFFERED_CAPABILITIES = 11;
    private static final int FIELD_DESIRED_CAPABILITIES = 12;
    private static final int FIELD_PROPERTIES = 13;

    public Attach(Object... fields) {
        super(14);
        int i = 0;
        for (Object field : fields) {
            getFields()[i++] = field;
        }
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Attach setName(Object o) {
        getFields()[FIELD_NAME] = o;
        return this;
    }

    public Attach setHandle(Object o) {
        getFields()[FIELD_HANDLE] = o;
        return this;
    }

    public Attach setRole(Object o) {
        getFields()[FIELD_ROLE] = o;
        return this;
    }

    public Attach setSndSettleMode(Object o) {
        getFields()[FIELD_SND_SETTLE_MODE] = o;
        return this;
    }

    public Attach setRcvSettleMode(Object o) {
        getFields()[FIELD_RCV_SETTLE_MODE] = o;
        return this;
    }

    public Attach setSource(Object o) {
        getFields()[FIELD_SOURCE] = o;
        return this;
    }

    public Attach setTarget(Object o) {
        getFields()[FIELD_TARGET] = o;
        return this;
    }

    public Attach setUnsettled(Object o) {
        getFields()[FIELD_UNSETTLED] = o;
        return this;
    }

    public Attach setIncompleteUnsettled(Object o) {
        getFields()[FIELD_INCOMPLETE_UNSETTLED] = o;
        return this;
    }

    public Attach setInitialDeliveryCount(Object o) {
        getFields()[FIELD_INITIAL_DELIVERY_COUNT] = o;
        return this;
    }

    public Attach setMaxMessageSize(Object o) {
        getFields()[FIELD_MAX_MESSAGE_SIZE] = o;
        return this;
    }

    public Attach setOfferedCapabilities(Object o) {
        getFields()[FIELD_OFFERED_CAPABILITIES] = o;
        return this;
    }

    public Attach setDesiredCapabilities(Object o) {
        getFields()[FIELD_DESIRED_CAPABILITIES] = o;
        return this;
    }

    public Attach setProperties(Object o) {
        getFields()[FIELD_PROPERTIES] = o;
        return this;
    }
}
