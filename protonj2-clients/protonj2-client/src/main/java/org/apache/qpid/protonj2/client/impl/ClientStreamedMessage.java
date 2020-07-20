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
package org.apache.qpid.protonj2.client.impl;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamedMessage;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * A {@link Message} implementation that exposes an interface that can manage
 * decode and access to messages sent in chunks from the remote.
 */
public class ClientStreamedMessage implements StreamedMessage {

    /**
     *
     */
    public ClientStreamedMessage() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public Header header() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> header(Header header) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeliveryAnnotations deliveryAnnotations() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> deliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageAnnotations messageAnnotations() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> messageAnnotations(MessageAnnotations messageAnnotations) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties properties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> properties(Properties properties) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ApplicationProperties applicationProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> applicationProperties(ApplicationProperties applicationProperties) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Footer footer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> footer(Footer footer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int messageFormat() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public AdvancedMessage<Object> messageFormat(int messageFormat) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer encode() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> addBodySection(Section<?> bodySection) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> bodySections(Collection<Section<?>> sections) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<Section<?>> bodySections() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean durable() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Message<Object> durable(boolean durable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte priority() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<Object> priority(byte priority) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long timeToLive() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<Object> timeToLive(long timeToLive) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean firstAcquirer() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Message<Object> firstAcquirer(boolean firstAcquirer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long deliveryCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<Object> deliveryCount(long deliveryCount) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object messageId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> messageId(Object messageId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] userId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> userId(byte[] userId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String to() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> to(String to) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String subject() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> subject(String subject) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String replyTo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> replyTo(String replyTo) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object correlationId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> correlationId(Object correlationId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String contentType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> contentType(String contentType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String contentEncoding() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<?> contentEncoding(String contentEncoding) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long absoluteExpiryTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<Object> absoluteExpiryTime(long expiryTime) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long creationTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<Object> creationTime(long createTime) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String groupId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> groupId(String groupId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int groupSequence() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<Object> groupSequence(int groupSequence) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String replyToGroupId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> replyToGroupId(String replyToGroupId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object deliveryAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasDeliveryAnnotation(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasDeliveryAnnotations() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeDeliveryAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> forEachDeliveryAnnotation(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> deliveryAnnotation(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object messageAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasMessageAnnotation(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasMessageAnnotations() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeMessageAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> forEachMessageAnnotation(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> messageAnnotation(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object applicationProperty(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasApplicationProperty(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasApplicationProperties() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeApplicationProperty(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> forEachApplicationProperty(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> applicationProperty(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object footer(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasFooter(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasFooters() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeFooter(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> forEachFooter(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<Object> footer(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object body() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isComplete() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int available() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public StreamedMessage readBytes(byte[] buffer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamedMessage readBytes(byte[] buffer, int offset, int length) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<Object> forEachBodySection(Consumer<Section<?>> consumer) {
        // TODO Auto-generated method stub
        return null;
    }
}
