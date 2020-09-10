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
 * Options class that controls various aspects of a {@link SendContext} instance.
 */
public class SendContextOptions {

    /**
     * Defines the default minimum size that the context write buffer will allocate
     * which drives the interval auto flushing of written data for this context.
     */
    public static final int MIN_BUFFER_SIZE_LIMIT = 256;

    private int messageFormat;
    private int bufferSize;

    /**
     * Creates a {@link SendContextOptions} instance with default values for all options
     */
    public SendContextOptions() {
    }

    /**
     * Create a {@link SendContextOptions} instance that copies all configuration from the given
     * {@link SendContextOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public SendContextOptions(SendContextOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link SendContextOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link SendContextOptions} class for chaining.
     */
    protected SendContextOptions copyInto(SendContextOptions other) {
        other.messageFormat(messageFormat);
        other.bufferSize(bufferSize);

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
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public SendContextOptions messageFormat(int messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    /**
     * @return the configured context write buffering limit for the associated {@link SendContext}
     */
    public int bufferSize() {
        return bufferSize;
    }

    /**
     * Sets the overall number of bytes the context will buffer before automatically flushing the
     * currently buffered bytes.  By default the context implementation chooses a value for this
     * buffer limited based on the configured frame size limits of the connection.
     *
     * @param bufferSize
     *       The number of bytes that can be written before the context performs a flush operation.
     *
     * @return this {@link SendContextOptions} instance.
     */
    public SendContextOptions bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }
}
