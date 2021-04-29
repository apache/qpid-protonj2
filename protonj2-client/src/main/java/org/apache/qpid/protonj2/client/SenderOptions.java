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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;

/**
 * Options that control the behavior of a {@link Sender} created from them.
 */
public class SenderOptions {

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private String linkName;
    private boolean autoSettle = true;
    private DeliveryMode deliveryMode = DeliveryMode.AT_LEAST_ONCE;

    private final SourceOptions source = new SourceOptions();
    private final TargetOptions target = new TargetOptions();

    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;

    /**
     * Create a new {@link SenderOptions} instance configured with default configuration settings.
     */
    public SenderOptions() {
    }

    /**
     * Create a new SenderOptions instance that copies the configuration from the specified source options.
     *
     * @param options
     * 		The SenderOptions instance whose settings are to be copied into this one.
     */
    public SenderOptions(SenderOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Configures the link name to use when creating a given {@link Sender} instance.
     *
     * @param linkName
     *      The assigned link name to use when creating a {@link Sender}.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions linkName(String linkName) {
        this.linkName = linkName;
        return this;
    }

    /**
     * @return the configured link name to use when creating a {@link Sender}.
     */
    public String linkName() {
        return linkName;
    }

    /**
     * Sets whether sent deliveries should be automatically locally-settled once
     * they have become remotely-settled by the receiving peer.
     *
     * True by default.
     *
     * @param autoSettle
     *            whether deliveries should be auto settled locally after being
     *            settled by the receiver
     *
     * @return the sender
     */
    public SenderOptions autoSettle(boolean autoSettle) {
        this.autoSettle = autoSettle;
        return this;
    }

    /**
     * Get whether the {@link Sender} is auto settling deliveries.
     *
     * @return whether deliveries should be auto settled locally after being settled
     *         by the receiver
     *
     * @see #autoSettle(boolean)
     */
    public boolean autoSettle() {
        return autoSettle;
    }

    /**
     * Sets the {@link DeliveryMode} value to assign to newly created {@link Sender} instances.
     *
     * @param deliveryMode
     *      The delivery mode value to configure.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions deliveryMode(DeliveryMode deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    /**
     * @return the current value of the {@link Sender} delivery mode configuration.
     */
    public DeliveryMode deliveryMode() {
        return deliveryMode;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a {@link Sender} is closed.
     */
    public long closeTimeout() {
        return closeTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * the {@link Sender} link.
     *
     * @param closeTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions closeTimeout(long closeTimeout) {
        return closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * the {@link Sender} link.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions closeTimeout(long timeout, TimeUnit units) {
        this.closeTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a {@link Sender} is opened.
     */
    public long openTimeout() {
        return openTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a {@link Sender} has been honored.
     *
     * @param openTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions openTimeout(long openTimeout) {
        return openTimeout(openTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a {@link Sender} has been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions openTimeout(long timeout, TimeUnit units) {
        this.openTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is message send.
     */
    public long sendTimeout() {
        return sendTimeout;
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param sendTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions sendTimeout(long sendTimeout) {
        return sendTimeout(sendTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions sendTimeout(long timeout, TimeUnit units) {
        this.sendTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource makes a request.
     */
    public long requestTimeout() {
        return requestTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to
     * perform some action such as starting a new transaction.  If the remote does not respond
     * within the configured timeout the resource making the request will mark it as failed and
     * return an error to the request initiator usually in the form of a
     * {@link ClientOperationTimedOutException}.
     *
     * @param requestTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions requestTimeout(long requestTimeout) {
        return requestTimeout(requestTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to
     * perform some action such as starting a new transaction.  If the remote does not respond
     * within the configured timeout the resource making the request will mark it as failed and
     * return an error to the request initiator usually in the form of a
     * {@link ClientOperationTimedOutException}.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions requestTimeout(long timeout, TimeUnit units) {
        this.requestTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the offeredCapabilities
     */
    public String[] offeredCapabilities() {
        return offeredCapabilities;
    }

    /**
     * @param offeredCapabilities the offeredCapabilities to set
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions offeredCapabilities(String... offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
        return this;
    }

    /**
     * @return the desiredCapabilities
     */
    public String[] desiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * @param desiredCapabilities the desiredCapabilities to set
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions desiredCapabilities(String... desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
        return this;
    }

    /**
     * @return the properties
     */
    public Map<String, Object> properties() {
        return properties;
    }

    /**
     * @param properties the properties to set
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return the source
     */
    public SourceOptions sourceOptions() {
        return source;
    }

    /**
     * @return the target
     */
    public TargetOptions targetOptions() {
        return target;
    }

    @Override
    public SenderOptions clone() {
        return copyInto(new SenderOptions());
    }

    /**
     * Copy all options from this {@link SenderOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    protected SenderOptions copyInto(SenderOptions other) {
        other.autoSettle(autoSettle);
        other.linkName(linkName);
        other.closeTimeout(closeTimeout);
        other.openTimeout(openTimeout);
        other.sendTimeout(sendTimeout);
        other.requestTimeout(requestTimeout);

        if (offeredCapabilities != null) {
            other.offeredCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (desiredCapabilities != null) {
            other.desiredCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (properties != null) {
            other.properties(new HashMap<>(properties));
        }

        source.copyInto(other.sourceOptions());
        target.copyInto(other.targetOptions());

        return this;
    }
}
