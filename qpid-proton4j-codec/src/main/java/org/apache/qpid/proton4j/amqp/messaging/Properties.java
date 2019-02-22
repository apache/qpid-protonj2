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
package org.apache.qpid.proton4j.amqp.messaging;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class Properties implements Section {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000073L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:properties:list");

    private static int MESSAGE_ID = 1;
    private static int USER_ID = 2;
    private static int TO = 4;
    private static int SUBJECT = 8;
    private static int REPLY_TO = 16;
    private static int CORRELATION_ID = 32;
    private static int CONTENT_TYPE = 64;
    private static int CONTENT_ENCODING = 128;
    private static int ABSOLUTE_EXPIRY = 256;
    private static int CREATION_TIME = 512;
    private static int GROUP_ID = 1024;
    private static int GROUP_SEQUENCE = 2048;
    private static int REPLY_TO_GROUP_ID = 4096;

    private int modified = 0;

    private Object messageId;
    private Binary userId;
    private String to;
    private String subject;
    private String replyTo;
    private Object correlationId;
    private String contentType;
    private String contentEncoding;
    private long absoluteExpiryTime;
    private long creationTime;
    private String groupId;
    private long groupSequence;
    private String replyToGroupId;

    public Properties() {
    }

    public Properties(Properties other) {
        this.messageId = other.messageId;
        this.userId = other.userId;
        this.to = other.to;
        this.subject = other.subject;
        this.replyTo = other.replyTo;
        this.correlationId = other.correlationId;
        this.contentType = other.contentType;
        this.contentEncoding = other.contentEncoding;
        this.absoluteExpiryTime = other.absoluteExpiryTime;
        this.creationTime = other.creationTime;
        this.groupId = other.groupId;
        this.groupSequence = other.groupSequence;
        this.replyToGroupId = other.replyToGroupId;
    }

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasMessageId() {
        return (modified & MESSAGE_ID) == MESSAGE_ID;
    }

    public boolean hasUserId() {
        return (modified & USER_ID) == USER_ID;
    }

    public boolean hasTo() {
        return (modified & TO) == TO;
    }

    public boolean hasSubject() {
        return (modified & SUBJECT) == SUBJECT;
    }

    public boolean hasReplyTo() {
        return (modified & REPLY_TO) == REPLY_TO;
    }

    public boolean hasCorrelationId() {
        return (modified & CORRELATION_ID) == CORRELATION_ID;
    }

    public boolean hasContentType() {
        return (modified & CONTENT_TYPE) == CONTENT_TYPE;
    }

    public boolean hasContentEncoding() {
        return (modified & CONTENT_ENCODING) == CONTENT_ENCODING;
    }

    public boolean hasAbsoluteExpiryTime() {
        return (modified & ABSOLUTE_EXPIRY) == ABSOLUTE_EXPIRY;
    }

    public boolean hasCreationTime() {
        return (modified & CREATION_TIME) == CREATION_TIME;
    }

    public boolean hasGroupId() {
        return (modified & GROUP_ID) == GROUP_ID;
    }

    public boolean hasGroupSequence() {
        return (modified & GROUP_SEQUENCE) == GROUP_SEQUENCE;
    }

    public boolean hasReplyToGroupId() {
        return (modified & REPLY_TO_GROUP_ID) == REPLY_TO_GROUP_ID;
    }

    //----- Access the AMQP Header object ------------------------------------//

    public Object getMessageId() {
        return messageId;
    }

    public void setMessageId(Object messageId) {
        if (messageId == null) {
            modified &= ~MESSAGE_ID;
        } else {
            modified |= MESSAGE_ID;
        }

        this.messageId = messageId;
    }

    public Binary getUserId() {
        return userId;
    }

    public void setUserId(Binary userId) {
        if (userId == null) {
            modified &= ~USER_ID;
        } else {
            modified |= USER_ID;
        }

        this.userId = userId;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        if (to == null) {
            modified &= ~TO;
        } else {
            modified |= TO;
        }

        this.to = to;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        if (subject == null) {
            modified &= ~SUBJECT;
        } else {
            modified |= SUBJECT;
        }

        this.subject = subject;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        if (replyTo == null) {
            modified &= ~REPLY_TO;
        } else {
            modified |= REPLY_TO;
        }

        this.replyTo = replyTo;
    }

    public Object getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(Object correlationId) {
        if (correlationId == null) {
            modified &= ~CORRELATION_ID;
        } else {
            modified |= CORRELATION_ID;
        }

        this.correlationId = correlationId;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        if (contentType == null) {
            modified &= ~CONTENT_TYPE;
        } else {
            modified |= CONTENT_TYPE;
        }

        this.contentType = contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public void setContentEncoding(String contentEncoding) {
        if (contentEncoding == null) {
            modified &= ~CONTENT_ENCODING;
        } else {
            modified |= CONTENT_ENCODING;
        }

        this.contentEncoding = contentEncoding;
    }

    public long getAbsoluteExpiryTime() {
        return absoluteExpiryTime;
    }

    public void setAbsoluteExpiryTime(long absoluteExpiryTime) {
        modified |= ABSOLUTE_EXPIRY;
        this.absoluteExpiryTime = absoluteExpiryTime;
    }

    public void clearAbsoluteExpiryTime() {
        modified &= ~ABSOLUTE_EXPIRY;
        absoluteExpiryTime = 0;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        modified |= CREATION_TIME;
        this.creationTime = creationTime;
    }

    public void clearCreationTime() {
        modified &= ~CREATION_TIME;
        creationTime = 0;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        if (groupId == null) {
            modified &= ~GROUP_ID;
        } else {
            modified |= GROUP_ID;
        }

        this.groupId = groupId;
    }

    public long getGroupSequence() {
        return groupSequence;
    }

    public void setGroupSequence(long groupSequence) {
        if (groupSequence < 0 || groupSequence > UnsignedInteger.MAX_VALUE.longValue()) {
            throw new IllegalArgumentException("Group Sequence value given is out of range: " + groupSequence);
        } else {
            modified |= GROUP_SEQUENCE;
        }

        this.groupSequence = groupSequence;
    }

    public void clearGroupSequence() {
        modified &= ~GROUP_SEQUENCE;
        groupSequence = 0;
    }

    public String getReplyToGroupId() {
        return replyToGroupId;
    }

    public void setReplyToGroupId(String replyToGroupId) {
        if (replyToGroupId == null) {
            modified &= ~REPLY_TO_GROUP_ID;
        } else {
            modified |= REPLY_TO_GROUP_ID;
        }

        this.replyToGroupId = replyToGroupId;
    }

    @Override
    public String toString() {
        return "Properties{" +
                "messageId=" + messageId +
                ", userId=" + userId +
                ", to='" + to + '\'' +
                ", subject='" + subject + '\'' +
                ", replyTo='" + replyTo + '\'' +
                ", correlationId=" + correlationId +
                ", contentType=" + contentType +
                ", contentEncoding=" + contentEncoding +
                ", absoluteExpiryTime=" + absoluteExpiryTime +
                ", creationTime=" + creationTime +
                ", groupId='" + groupId + '\'' +
                ", groupSequence=" + groupSequence +
                ", replyToGroupId='" + replyToGroupId + '\'' + " }";
    }

    @Override
    public SectionType getType() {
        return SectionType.Properties;
    }
}
