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

import java.util.concurrent.TimeUnit;

/**
 * Options class that controls various aspects of a {@link StreamReceiver} instance and how
 * a streamed message transfer is written.
 */
public final class StreamReceiverOptions extends LinkOptions<StreamReceiverOptions> implements Cloneable {

    /**
     * Defines the default read buffering size which is used to control how much incoming
     * data can be buffered before the remote has back pressured applied to avoid out of
     * memory conditions.
     */
    public static final int DEFAULT_READ_BUFFER_SIZE = SessionOptions.DEFAULT_SESSION_INCOMING_CAPACITY;

    private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
    private long drainTimeout = ConnectionOptions.DEFAULT_DRAIN_TIMEOUT;
    private boolean autoAccept = true;
    private int creditWindow = 10;

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

    @Override
    public StreamReceiverOptions clone() {
        return copyInto(new StreamReceiverOptions());
    }

    /**
     * Copy all options from this {@link StreamReceiverOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return the {@link StreamReceiverOptions} instance that was given.
     */
    protected StreamReceiverOptions copyInto(StreamReceiverOptions other) {
        super.copyInto(other);

        other.readBufferSize(readBufferSize);
        other.autoAccept(autoAccept);
        other.creditWindow(creditWindow);
        other.drainTimeout(drainTimeout);

        return other;
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

    /**
     * Controls if the created Receiver will automatically accept the deliveries that have
     * been received by the application (default is <code>true</code>).
     *
     * @param autoAccept
     *      The value to assign for auto delivery acceptance.
     *
     * @return this {@link StreamReceiverOptions} instance.
     */
    public StreamReceiverOptions autoAccept(boolean autoAccept) {
        this.autoAccept = autoAccept;
        return this;
    }

    /**
     * @return the current value of the {@link Receiver} auto accept setting.
     */
    public boolean autoAccept() {
        return autoAccept;
    }

    /**
     * @return the credit window configuration that will be applied to created {@link Receiver} instances.
     */
    public int creditWindow() {
        return creditWindow;
    }

    /**
     * A credit window value that will be used to maintain an window of credit for Receiver instances
     * that are created.  The {@link Receiver} will allow up to the credit window amount of incoming
     * deliveries to be queued and as they are read from the {@link Receiver} the window will be extended
     * to maintain a consistent backlog of deliveries.  The default is to configure a credit window of 10.
     * <p>
     * To disable credit windowing and allow the client application to control the credit on the {@link Receiver}
     * link the credit window value should be set to zero.
     *
     * @param creditWindow
     *      The assigned credit window value to use.
     *
     * @return this {@link StreamReceiverOptions} instance.
     */
    public StreamReceiverOptions creditWindow(int creditWindow) {
        this.creditWindow = creditWindow;
        return this;
    }

    /**
     * @return the configured drain timeout value that will use to fail a pending drain request.
     */
    public long drainTimeout() {
        return drainTimeout;
    }

    /**
     * Sets the drain timeout (in milliseconds) after which a {@link Receiver} request to drain
     * link credit is considered failed and the request will be marked as such.
     *
     * @param drainTimeout
     *      the drainTimeout to use for receiver links.
     *
     * @return this {@link StreamReceiverOptions} instance.
     */
    public StreamReceiverOptions drainTimeout(long drainTimeout) {
        return drainTimeout(drainTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the drain timeout value after which a {@link Receiver} request to drain
     * link credit is considered failed and the request will be marked as such.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link StreamReceiverOptions} instance.
     */
    public StreamReceiverOptions drainTimeout(long timeout, TimeUnit units) {
        this.drainTimeout = units.toMillis(timeout);
        return this;
    }

    @Override
    protected StreamReceiverOptions self() {
        return this;
    }
}
