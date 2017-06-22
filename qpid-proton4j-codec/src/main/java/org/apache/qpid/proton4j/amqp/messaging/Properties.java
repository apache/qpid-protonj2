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

import java.util.Date;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;

public final class Properties implements Section {

    private Object messageId;
    private Binary userId;
    private String to;
    private String subject;
    private String replyTo;
    private Object correlationId;
    private Symbol contentType;
    private Symbol contentEncoding;
    private Date absoluteExpiryTime;
    private Date creationTime;
    private String groupId;
    private UnsignedInteger groupSequence;
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

    public Object getMessageId() {
        return messageId;
    }

    public void setMessageId(Object messageId) {
        this.messageId = messageId;
    }

    public Binary getUserId() {
        return userId;
    }

    public void setUserId(Binary userId) {
        this.userId = userId;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public Object getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(Object correlationId) {
        this.correlationId = correlationId;
    }

    public Symbol getContentType() {
        return contentType;
    }

    public void setContentType(Symbol contentType) {
        this.contentType = contentType;
    }

    public Symbol getContentEncoding() {
        return contentEncoding;
    }

    public void setContentEncoding(Symbol contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    public Date getAbsoluteExpiryTime() {
        return absoluteExpiryTime;
    }

    public void setAbsoluteExpiryTime(Date absoluteExpiryTime) {
        this.absoluteExpiryTime = absoluteExpiryTime;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public UnsignedInteger getGroupSequence() {
        return groupSequence;
    }

    public void setGroupSequence(UnsignedInteger groupSequence) {
        this.groupSequence = groupSequence;
    }

    public String getReplyToGroupId() {
        return replyToGroupId;
    }

    public void setReplyToGroupId(String replyToGroupId) {
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
}
