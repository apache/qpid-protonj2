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
package org.apache.qpid.proton4j.test.driver.codec.messaging;

import java.util.Date;
import java.util.List;

import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Binary;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedLong;

public class Properties extends ListDescribedType {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000073L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:properties:list");

    /**
     * Enumeration which maps to fields in the Properties Performative
     */
    public enum Field {
        MESSAGE_ID,
        USER_ID,
        TO,
        SUBJECT,
        REPLY_TO,
        CORRELATION_ID,
        CONTENT_TYPE,
        CONTENT_ENCODING,
        ABSOLUTE_EXPIRY_TIME,
        CREATION_TIME,
        GROUP_ID,
        GROUP_SEQUENCE,
        REPLY_TO_GROUP_ID,
    }

    public Properties() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Properties(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Properties(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Properties setMessageId(Object o) {
        getList().set(Field.MESSAGE_ID.ordinal(), o);
        return this;
    }

    public Object getMessageId() {
        return getList().get(Field.MESSAGE_ID.ordinal());
    }

    public Properties setUserId(Binary o) {
        getList().set(Field.USER_ID.ordinal(), o);
        return this;
    }

    public Binary getUserId() {
        return (Binary) getList().get(Field.USER_ID.ordinal());
    }

    public Properties setTo(String o) {
        getList().set(Field.TO.ordinal(), o);
        return this;
    }

    public String getTo() {
        return (String) getList().get(Field.TO.ordinal());
    }

    public Properties setSubject(String o) {
        getList().set(Field.SUBJECT.ordinal(), o);
        return this;
    }

    public String getSubject() {
        return (String) getList().get(Field.SUBJECT.ordinal());
    }

    public Properties setReplyTo(String o) {
        getList().set(Field.REPLY_TO.ordinal(), o);
        return this;
    }

    public String getReplyTo() {
        return (String) getList().get(Field.REPLY_TO.ordinal());
    }

    public Properties setCorrelationId(Object o) {
        getList().set(Field.CORRELATION_ID.ordinal(), o);
        return this;
    }

    public Object getCorrelationId() {
        return getList().get(Field.CORRELATION_ID.ordinal());
    }

    public Properties setContentType(Symbol o) {
        getList().set(Field.CONTENT_TYPE.ordinal(), o);
        return this;
    }

    public Symbol getContentType() {
        return (Symbol) getList().get(Field.CONTENT_TYPE.ordinal());
    }

    public Properties setContentEncoding(Symbol o) {
        getList().set(Field.CONTENT_ENCODING.ordinal(), o);
        return this;
    }

    public Symbol getContentEncoding() {
        return (Symbol) getList().get(Field.CONTENT_ENCODING.ordinal());
    }

    public Properties setAbsoluteExpiryTime(Date o) {
        getList().set(Field.ABSOLUTE_EXPIRY_TIME.ordinal(), o);
        return this;
    }

    public Date getAbsoluteExpiryTime() {
        return (Date) getList().get(Field.ABSOLUTE_EXPIRY_TIME.ordinal());
    }

    public Properties setCreationTime(Date o) {
        getList().set(Field.CREATION_TIME.ordinal(), o);
        return this;
    }

    public Date getCreationTime() {
        return (Date) getList().get(Field.CREATION_TIME.ordinal());
    }

    public Properties setGroupId(String o) {
        getList().set(Field.GROUP_ID.ordinal(), o);
        return this;
    }

    public String getGroupId() {
        return (String) getList().get(Field.GROUP_ID.ordinal());
    }

    public Properties setGroupSequence(UnsignedInteger o) {
        getList().set(Field.GROUP_SEQUENCE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getGroupSequence() {
        return (UnsignedInteger) getList().get(Field.GROUP_SEQUENCE.ordinal());
    }

    public Properties setReplyToGroupId(String o) {
        getList().set(Field.REPLY_TO_GROUP_ID.ordinal(), o);
        return this;
    }

    public String getReplyToGroupId() {
        return (String) getList().get(Field.REPLY_TO_GROUP_ID.ordinal());
    }
}
