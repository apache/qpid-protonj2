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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.AMQPHeader.HeaderHandler;

/**
 * Envelope type that carries AMQPHeader instances
 */
public class HeaderEnvelope extends PerformativeEnvelope<AMQPHeader> {

    /**
     * The frame type marker used when decoding or encoding AMQP Header frames.
     */
    public static final byte HEADER_FRAME_TYPE = (byte) 1;

    /**
     * A singleton instance of an SASL header that can be used to avoid additional allocations.
     */
    public static final HeaderEnvelope SASL_HEADER_ENVELOPE = new HeaderEnvelope(AMQPHeader.getSASLHeader());

    /**
     * A singleton instance of an AMQP header that can be used to avoid additional allocations.
     */
    public static final HeaderEnvelope AMQP_HEADER_ENVELOPE = new HeaderEnvelope(AMQPHeader.getAMQPHeader());

    /**
     * Create an header envelope with the given AMQHeader body.
     *
     * @param body
     * 		The AMQP header instance that holds the header bytes.
     */
    public HeaderEnvelope(AMQPHeader body) {
        super(HEADER_FRAME_TYPE);

        initialize(body, 0, null);
    }

    /**
     * @return the protocol id value contained in the header bytes.
     */
    public int getProtocolId() {
        return getBody().getProtocolId();
    }

    /**
     * @return the major version number contained in the header bytes.
     */
    public int getMajor() {
        return getBody().getMajor();
    }

    /**
     * @return the minor version number contained in the header bytes.
     */
    public int getMinor() {
        return getBody().getMinor();
    }

    /**
     * @return the revision version number contained in the header bytes.
     */
    public int getRevision() {
        return getBody().getRevision();
    }

    /**
     * @return true if the contained header bytes represents a SASL header.
     */
    public boolean isSaslHeader() {
        return getBody().isSaslHeader();
    }

    /**
     * Invoke the correct handler based on whether this header is a SASL or AMQP header instance.
     *
     * @param <E> The type of the context object that accompanies the invocation.
     *
     * @param handler
     * 		The {@link HeaderHandler} that should be invoked based on the header type.
     * @param context
     * 		The context value to provide when signaling the header handler.
     */
    public <E> void invoke(HeaderHandler<E> handler, E context) {
        getBody().invoke(handler, context);
    }
}
