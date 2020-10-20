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
 * Options class that controls various aspects of a {@link StreamReceiver} instance and how
 * a streamed message transfer is written.
 */
public class StreamReceiverOptions extends ReceiverOptions {

    /**
     * Defines the default read buffering size which is used to control how much incoming
     * data can be buffered before the remote has back pressured applied to avoid out of
     * memory conditions.
     */
    public static final int DEFAULT_READ_BUFFER_SIZE = 50 * 1024 * 1024;

    private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;

    /**
     * Creates a {@link StreamReceiverOptions} instance with default values for all options
     */
    public StreamReceiverOptions() {
    }

    /**
     * Create a {@link StreamReceiverOptions} instance that copies all configuration from the given
     * {@link StreamReceiverOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public StreamReceiverOptions(StreamReceiverOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link StreamReceiverOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link StreamReceiverOptions} class for chaining.
     */
    protected StreamReceiverOptions copyInto(StreamReceiverOptions other) {
        super.copyInto(other);

        other.readBufferSize(readBufferSize);

        return this;
    }

    /**
     * @return the configured session capacity for the parent session of the {@link StreamReceiver}.
     */
    public int readBufferSize() {
        return readBufferSize;
    }

    /**
     * Sets the incoming buffer capacity (in bytes) that the {@link StreamReceiver}.
     * <p>
     * When the remote peer is sending incoming data for a {@link StreamReceiverMessage} the amount that is stored
     * in memory before back pressure is applied to the remote is controlled by this option.  If the user
     * does not read incoming data as it arrives this limit can prevent out of memory errors that might
     * otherwise arise as the remote attempts to immediately send all contents of very large message payloads.
     *
     * @param readBufferSize
     *       The number of bytes that the {@link StreamReceiver} will buffer for a given {@link StreamReceiverMessage}.
     *
     * @return this {@link StreamReceiverOptions} instance.
     */
    public StreamReceiverOptions readBufferSize(int readBufferSize) {
        this.readBufferSize = readBufferSize;
        return this;
    }
}
