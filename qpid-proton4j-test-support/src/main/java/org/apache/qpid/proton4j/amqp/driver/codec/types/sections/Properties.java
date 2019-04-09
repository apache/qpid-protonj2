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

import java.util.List;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;

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

    public Properties setUserId(Object o) {
        getList().set(Field.USER_ID.ordinal(), o);
        return this;
    }

    public Object getUserId() {
        return getList().get(Field.USER_ID.ordinal());
    }

    public Properties setTo(Object o) {
        getList().set(Field.TO.ordinal(), o);
        return this;
    }

    public Object getTo() {
        return getList().get(Field.TO.ordinal());
    }

    public Properties setSubject(Object o) {
        getList().set(Field.SUBJECT.ordinal(), o);
        return this;
    }

    public Object getSubject() {
        return getList().get(Field.SUBJECT.ordinal());
    }

    public Properties setReplyTo(Object o) {
        getList().set(Field.REPLY_TO.ordinal(), o);
        return this;
    }

    public Object getReplyTo() {
        return getList().get(Field.REPLY_TO.ordinal());
    }

    public Properties setCorrelationId(Object o) {
        getList().set(Field.CORRELATION_ID.ordinal(), o);
        return this;
    }

    public Object getCorrelationId() {
        return getList().get(Field.CORRELATION_ID.ordinal());
    }

    public Properties setContentType(Object o) {
        getList().set(Field.CONTENT_TYPE.ordinal(), o);
        return this;
    }

    public Object getContentType() {
        return getList().get(Field.CONTENT_TYPE.ordinal());
    }

    public Properties setContentEncoding(Object o) {
        getList().set(Field.CONTENT_ENCODING.ordinal(), o);
        return this;
    }

    public Object getContentEncoding() {
        return getList().get(Field.CONTENT_ENCODING.ordinal());
    }

    public Properties setAbsoluteExpiryTime(Object o) {
        getList().set(Field.ABSOLUTE_EXPIRY_TIME.ordinal(), o);
        return this;
    }

    public Object getAbsoluteExpiryTime() {
        return getList().get(Field.ABSOLUTE_EXPIRY_TIME.ordinal());
    }

    public Properties setCreationTime(Object o) {
        getList().set(Field.CREATION_TIME.ordinal(), o);
        return this;
    }

    public Object getCreationTime() {
        return getList().get(Field.CREATION_TIME.ordinal());
    }

    public Properties setGroupId(Object o) {
        getList().set(Field.GROUP_ID.ordinal(), o);
        return this;
    }

    public Object getGroupId() {
        return getList().get(Field.GROUP_ID.ordinal());
    }

    public Properties setGroupSequence(Object o) {
        getList().set(Field.GROUP_SEQUENCE.ordinal(), o);
        return this;
    }

    public Object getGroupSequence() {
        return getList().get(Field.GROUP_SEQUENCE.ordinal());
    }

    public Properties setReplyToGroupId(Object o) {
        getList().set(Field.REPLY_TO_GROUP_ID.ordinal(), o);
        return this;
    }

    public Object getReplyToGroupId() {
        return getList().get(Field.REPLY_TO_GROUP_ID.ordinal());
    }
}
