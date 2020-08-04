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
package org.apache.qpid.protonj2.client;

import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Options class that controls various aspects of a {@link MessageOutputStream} instance.
 */
public class RawOutputStreamOptions {

    private int messageFormat;
    private int streamBufferLimit;

    /**
     * Creates a {@link RawOutputStreamOptions} instance with default values for all options
     */
    public RawOutputStreamOptions() {
    }

    /**
     * Create a {@link RawOutputStreamOptions} instance that copies all configuration from the given
     * {@link RawOutputStreamOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public RawOutputStreamOptions(RawOutputStreamOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link RawOutputStreamOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link RawOutputStreamOptions} class for chaining.
     */
    protected RawOutputStreamOptions copyInto(RawOutputStreamOptions other) {
        other.messageFormat(messageFormat);
        other.streamBufferLimit(streamBufferLimit);

        return this;
    }

    /**
     * Returns the configured message format value that will be set on the first outgoing
     * AMQP {@link Transfer} frame for the delivery that comprises this streamed message.
     *
     * @return the configured message format that will be sent.
     */
    public int messageFormat() {
        return messageFormat;
    }

    /**
     * Sets the message format value to use when writing the first AMQP {@link Transfer} frame
     * for this streamed message.  If not set the default value (0) is used for the message.
     *
     * @param messageFormat
     *      The message format value to use when streaming the message data.
     *
     * @return this {@link RawOutputStreamOptions} instance.
     */
    public RawOutputStreamOptions messageFormat(int messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

   /**
    * @return the configured stream write buffering limit for the associated {@link MessageOutputStream}
    */
   public int streamBufferLimit() {
       return streamBufferLimit;
   }

   /**
    * Sets the overall number of bytes the stream will buffer before automatically flushing the
    * currently buffered bytes.  By default the stream implementation chooses a value for this
    * buffer limited based on the negotiated max AMQP frame size from the remote
    *
    * @param streamBufferLimit
    *       The number of bytes that can be written before the stream performs a flush operation.
    *
    * @return this {@link RawOutputStreamOptions} instance.
    */
   public RawOutputStreamOptions streamBufferLimit(int streamBufferLimit) {
       this.streamBufferLimit = streamBufferLimit;
       return this;
   }
}