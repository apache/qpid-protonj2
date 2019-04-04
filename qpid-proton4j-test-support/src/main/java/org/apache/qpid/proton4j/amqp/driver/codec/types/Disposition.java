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

public class Disposition extends ListDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:disposition:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000015L);

    private static final int FIELD_ROLE = 0;
    private static final int FIELD_FIRST = 1;
    private static final int FIELD_LAST = 2;
    private static final int FIELD_SETTLED = 3;
    private static final int FIELD_STATE = 4;
    private static final int FIELD_BATCHABLE = 5;

    public Disposition(Object... fields) {
        super(6);
        int i = 0;
        for (Object field : fields) {
            getFields()[i++] = field;
        }
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Disposition setRole(Object o) {
        getFields()[FIELD_ROLE] = o;
        return this;
    }

    public Disposition setFirst(Object o) {
        getFields()[FIELD_FIRST] = o;
        return this;
    }

    public Disposition setLast(Object o) {
        getFields()[FIELD_LAST] = o;
        return this;
    }

    public Disposition setSettled(Object o) {
        getFields()[FIELD_SETTLED] = o;
        return this;
    }

    public Disposition setState(Object o) {
        getFields()[FIELD_STATE] = o;
        return this;
    }

    public Disposition setBatchable(Object o) {
        getFields()[FIELD_BATCHABLE] = o;
        return this;
    }
}
