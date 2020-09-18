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

/**
 * Options class that controls various aspects of a {@link StreamTracker} instance and how
 * a streamed message transfer is written.
 */
public class StreamSenderOptions extends SenderOptions {

    /**
     * Defines the default minimum size that the context write buffer will allocate
     * which drives the interval auto flushing of written data for this context.
     */
    public static final int MIN_BUFFER_SIZE_LIMIT = 256;

    private int writeBufferSize;

    /**
     * Creates a {@link StreamSenderOptions} instance with default values for all options
     */
    public StreamSenderOptions() {
    }

    /**
     * Create a {@link StreamSenderOptions} instance that copies all configuration from the given
     * {@link StreamSenderOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public StreamSenderOptions(StreamSenderOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link StreamSenderOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link StreamSenderOptions} class for chaining.
     */
    protected StreamSenderOptions copyInto(StreamSenderOptions other) {
        super.copyInto(other);

        other.writeBufferSize(writeBufferSize);

        return this;
    }

    /**
     * @return the configured context write buffering limit for the associated {@link SendContext}
     */
    public int writeBufferSize() {
        return writeBufferSize;
    }

    /**
     * Sets the overall number of bytes the context will buffer before automatically flushing the
     * currently buffered bytes.  By default the context implementation chooses a value for this
     * buffer limited based on the configured frame size limits of the connection.
     *
     * @param writeBufferSize
     *       The number of bytes that can be written before the context performs a flush operation.
     *
     * @return this {@link StreamSenderOptions} instance.
     */
    public StreamSenderOptions writeBufferSize(int writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
        return this;
    }
}
