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
package org.apache.qpid.proton4j.amqp;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * A simple default DeliveryTag implementation.
 */
public class DefaultDeliveryTag implements DeliveryTag {

    private final byte[] tag;
    private final int tagOffset;
    private final int tagLength;

    private Binary cachedBinary;

    public DefaultDeliveryTag(byte[] tag) {
        this(tag, 0, tag.length);
    }

    public DefaultDeliveryTag(byte[] tag, int tagOffset, int tagLength) {
        this.tag = tag;
        this.tagOffset = tagOffset;
        this.tagLength = tagLength;
    }

    public DefaultDeliveryTag(Binary tag) {
        this(tag.getArray(), tag.getArrayOffset(), tag.getLength());

        this.cachedBinary = tag;
    }

    @Override
    public Binary toBinary() {
        if (cachedBinary == null) {
            cachedBinary = new Binary(tag, tagOffset, tagLength);
        }

        return cachedBinary;
    }

    @Override
    public void release() {
        cachedBinary = null;
    }

    @Override
    public void writeTo(ProtonBuffer buffer) {
        buffer.writeBytes(tag, tagOffset, tagLength);
    }
}
