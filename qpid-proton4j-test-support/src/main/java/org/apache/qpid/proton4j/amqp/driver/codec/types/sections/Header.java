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
package org.apache.qpid.proton4j.amqp.driver.codec.types.sections;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

public class Header extends ListDescribedType {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000070L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:header:list");

    private static final int FIELD_DURABLE = 0;
    private static final int FIELD_PRIORITY = 1;
    private static final int FIELD_TTL = 2;
    private static final int FIELD_FIRST_ACQUIRER = 3;
    private static final int FIELD_DELIVERY_COUNT = 4;

    public Header(Object... fields) {
        super(5);
        int i = 0;
        for (Object field : fields) {
            getFields()[i++] = field;
        }
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Header setDurable(Object o) {
        getFields()[FIELD_DURABLE] = o;
        return this;
    }

    public Header setPriority(Object o) {
        getFields()[FIELD_PRIORITY] = o;
        return this;
    }

    public Header setTtl(Object o) {
        getFields()[FIELD_TTL] = o;
        return this;
    }

    public Header setFirstAcquirer(Object o) {
        getFields()[FIELD_FIRST_ACQUIRER] = o;
        return this;
    }

    public Header setDeliveryCount(Object o) {
        getFields()[FIELD_DELIVERY_COUNT] = o;
        return this;
    }
}
