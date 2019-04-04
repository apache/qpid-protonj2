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

public class Properties extends ListDescribedType {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000073L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:properties:list");

    private static final int FIELD_MESSAGE_ID = 0;
    private static final int FIELD_USER_ID = 1;
    private static final int FIELD_TO = 2;
    private static final int FIELD_SUBJECT = 3;
    private static final int FIELD_REPLY_TO = 4;
    private static final int FIELD_CORRELATION_ID = 5;
    private static final int FIELD_CONTENT_TYPE = 6;
    private static final int FIELD_CONTENT_ENCODING = 7;
    private static final int FIELD_ABSOLUTE_EXPIRY_TIME = 8;
    private static final int FIELD_CREATION_TIME = 9;
    private static final int FIELD_GROUP_ID = 10;
    private static final int FIELD_GROUP_SEQUENCE = 11;
    private static final int FIELD_REPLY_TO_GROUP_ID = 12;

    public Properties(Object... fields) {
        super(13);
        int i = 0;
        for (Object field : fields) {
            getFields()[i++] = field;
        }
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Properties setMessageId(Object o) {
        getFields()[FIELD_MESSAGE_ID] = o;
        return this;
    }

    public Properties setUserId(Object o) {
        getFields()[FIELD_USER_ID] = o;
        return this;
    }

    public Properties setTo(Object o) {
        getFields()[FIELD_TO] = o;
        return this;
    }

    public Properties setSubject(Object o) {
        getFields()[FIELD_SUBJECT] = o;
        return this;
    }

    public Properties setReplyTo(Object o) {
        getFields()[FIELD_REPLY_TO] = o;
        return this;
    }

    public Properties setCorrelationId(Object o) {
        getFields()[FIELD_CORRELATION_ID] = o;
        return this;
    }

    public Properties setContentType(Object o) {
        getFields()[FIELD_CONTENT_TYPE] = o;
        return this;
    }

    public Properties setContentEncoding(Object o) {
        getFields()[FIELD_CONTENT_ENCODING] = o;
        return this;
    }

    public Properties setAbsoluteExpiryTime(Object o) {
        getFields()[FIELD_ABSOLUTE_EXPIRY_TIME] = o;
        return this;
    }

    public Properties setCreationTime(Object o) {
        getFields()[FIELD_CREATION_TIME] = o;
        return this;
    }

    public Properties setGroupId(Object o) {
        getFields()[FIELD_GROUP_ID] = o;
        return this;
    }

    public Properties setGroupSequence(Object o) {
        getFields()[FIELD_GROUP_SEQUENCE] = o;
        return this;
    }

    public Properties setReplyToGroupId(Object o) {
        getFields()[FIELD_REPLY_TO_GROUP_ID] = o;
        return this;
    }
}
