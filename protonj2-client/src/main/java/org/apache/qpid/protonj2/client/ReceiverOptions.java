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

/**
 * Options that control the behavior of the {@link Receiver} created from them.
 */
public class ReceiverOptions {

    private long drainTimeout = ConnectionOptions.DEFAULT_DRAIN_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private boolean autoAccept = true;
    private boolean autoSettle = true;
    private DeliveryMode deliveryMode = DeliveryMode.AT_LEAST_ONCE;
    private int creditWindow = 10;
    private String linkName;

    private final SourceOptions source = new SourceOptions();
    private final TargetOptions target = new TargetOptions();

    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;

    /**
     * Create a new ReceiverOptions instance with defaults set for all options.
     */
    public ReceiverOptions() {
    }

    /**
     * Create a new ReceiverOptions instance that copies the configuration from the specified source options.
     *
     * @param options
     * 		The ReceiverOptions instance whose settings are to be copied into this one.
     */
    public ReceiverOptions(ReceiverOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Controls if the created Receiver will automatically accept the deliveries that have
     * been received by the application (default is <code>true</code>).
     *
     * @param autoAccept
     *      The value to assign for auto delivery acceptance.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions autoAccept(boolean autoAccept) {
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
     * Controls if the created Receiver will automatically settle the deliveries that have
     * been received by the application (default is <code>true</code>).
     *
     * @param autoSettle
     *      The value to assign for auto delivery settlement.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions autoSettle(boolean autoSettle) {
        this.autoSettle = autoSettle;
        return this;
    }

    /**
     * @return the current value of the {@link Receiver} auto settlement setting.
     */
    public boolean autoSettle() {
        return autoSettle;
    }

    /**
     * Sets the {@link DeliveryMode} value to assign to newly created {@link Receiver} instances.
     *
     * @param deliveryMode
     *      The delivery mode value to configure.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions deliveryMode(DeliveryMode deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    /**
     * @return the current value of the {@link Receiver} delivery mode configuration.
     */
    public DeliveryMode deliveryMode() {
        return deliveryMode;
    }

    /**
     * Configures the link name to use when creating a given {@link Receiver} instance.
     *
     * @param linkName
     *      The assigned link name to use when creating a {@link Receiver}.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions linkName(String linkName) {
        this.linkName = linkName;
        return this;
    }

    /**
     * @return the configured link name to use when creating a {@link Receiver}.
     */
    public String linkName() {
        return linkName;
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions creditWindow(int creditWindow) {
        this.creditWindow = creditWindow;
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a {@link Receiver} is closed.
     */
    public long closeTimeout() {
        return closeTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * the {@link Receiver} link.
     *
     * @param closeTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions closeTimeout(long closeTimeout) {
        return closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * the {@link Receiver} link.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions closeTimeout(long timeout, TimeUnit units) {
        this.closeTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a {@link Receiver} is opened.
     */
    public long openTimeout() {
        return openTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a {@link Receiver} has been honored.
     *
     * @param openTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions openTimeout(long openTimeout) {
        return openTimeout(openTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a {@link Receiver} has been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions openTimeout(long timeout, TimeUnit units) {
        this.openTimeout = units.toMillis(timeout);
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions drainTimeout(long drainTimeout) {
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions drainTimeout(long timeout, TimeUnit units) {
        this.drainTimeout = units.toMillis(timeout);
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions requestTimeout(long requestTimeout) {
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions requestTimeout(long timeout, TimeUnit units) {
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions offeredCapabilities(String... offeredCapabilities) {
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions desiredCapabilities(String... desiredCapabilities) {
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
     * @return this {@link ReceiverOptions} instance.
     */
    public ReceiverOptions properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return the source options that will be used when creating new {@link Receiver} instances.
     */
    public SourceOptions sourceOptions() {
        return source;
    }

    /**
     * @return the target options that will be used when creating new {@link Sender} instances.
     */
    public TargetOptions targetOptions() {
        return target;
    }

    @Override
    public ReceiverOptions clone() {
        return copyInto(new ReceiverOptions());
    }

    /**
     * Copy all options from this {@link ReceiverOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    protected ReceiverOptions copyInto(ReceiverOptions other) {
        other.creditWindow(creditWindow);
        other.linkName(linkName);
        other.closeTimeout(closeTimeout);
        other.openTimeout(openTimeout);
        other.drainTimeout(drainTimeout);
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
