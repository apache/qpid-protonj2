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
 * Base options that are applied to AMQP link types.
 *
 * @param <T> The actual {@link LinkOptions} concrete type (SenderOptions or ReceiverOptions).
 */
public abstract class LinkOptions<T extends LinkOptions<T>> {

    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private boolean autoSettle = true;
    private DeliveryMode deliveryMode = DeliveryMode.AT_LEAST_ONCE;
    private String linkName;

    private final SourceOptions source = new SourceOptions();
    private final TargetOptions target = new TargetOptions();

    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;

    /**
     * Create a new LinkOptions instance with defaults set for all options.
     */
    public LinkOptions() {
    }

    /**
     * Create a new LinkOptions instance that copies the configuration from the specified source options.
     *
     * @param options
     * 		The LinkOptions instance whose settings are to be copied into this one.
     */
    public LinkOptions(LinkOptions<T> options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Controls if the created Link will automatically settle the deliveries that have
     * been received by the application (default is <code>true</code>). This option will
     * also result in an accepted outcome being applied to the settled delivery.
     *
     * @param autoSettle
     *      The value to assign for auto delivery settlement.
     *
     * @return this {@link LinkOptions} instance.
     */
    public T autoSettle(boolean autoSettle) {
        this.autoSettle = autoSettle;
        return self();
    }

    /**
     * @return the current value of the {@link Link} auto settlement setting.
     */
    public boolean autoSettle() {
        return autoSettle;
    }

    /**
     * Sets the {@link DeliveryMode} value to assign to newly created {@link Link} instances.
     *
     * @param deliveryMode
     *      The delivery mode value to configure.
     *
     * @return this {@link LinkOptions} instance.
     */
    public T deliveryMode(DeliveryMode deliveryMode) {
        this.deliveryMode = deliveryMode;
        return self();
    }

    /**
     * @return the current value of the {@link Link} delivery mode configuration.
     */
    public DeliveryMode deliveryMode() {
        return deliveryMode;
    }

    /**
     * Configures the link name to use when creating a given {@link Link} instance.
     *
     * @param linkName
     *      The assigned link name to use when creating a {@link Link}.
     *
     * @return this {@link LinkOptions} instance.
     */
    public T linkName(String linkName) {
        this.linkName = linkName;
        return self();
    }

    /**
     * @return the configured link name to use when creating a {@link Link}.
     */
    public String linkName() {
        return linkName;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a {@link Link} is closed.
     */
    public long closeTimeout() {
        return closeTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * the {@link Link} link.
     *
     * @param closeTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ReceiverOptions} instance.
     */
    public T closeTimeout(long closeTimeout) {
        return closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * the {@link Link} link.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link LinkOptions} instance.
     */
    public T closeTimeout(long timeout, TimeUnit units) {
        this.closeTimeout = units.toMillis(timeout);
        return self();
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a {@link Link} is opened.
     */
    public long openTimeout() {
        return openTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a {@link Link} has been honored.
     *
     * @param openTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link LinkOptions} instance.
     */
    public T openTimeout(long openTimeout) {
        return openTimeout(openTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a {@link Link} has been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link LinkOptions} instance.
     */
    public T openTimeout(long timeout, TimeUnit units) {
        this.openTimeout = units.toMillis(timeout);
        return self();
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
     * @return this {@link LinkOptions} instance.
     */
    public T requestTimeout(long requestTimeout) {
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
     * @return this {@link LinkOptions} instance.
     */
    public T requestTimeout(long timeout, TimeUnit units) {
        this.requestTimeout = units.toMillis(timeout);
        return self();
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
     * @return this {@link LinkOptions} instance.
     */
    public T offeredCapabilities(String... offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
        return self();
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
     * @return this {@link LinkOptions} instance.
     */
    public T desiredCapabilities(String... desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
        return self();
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
     * @return this {@link LinkOptions} instance.
     */
    public T properties(Map<String, Object> properties) {
        this.properties = properties;
        return self();
    }

    /**
     * @return the source options that will be used when creating new {@link Link} instances.
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

    /**
     * Copy all options from this {@link LinkOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    protected LinkOptions<T> copyInto(LinkOptions<T> other) {
        other.linkName(linkName);
        other.closeTimeout(closeTimeout);
        other.openTimeout(openTimeout);
        other.requestTimeout(requestTimeout);
        other.deliveryMode(deliveryMode);
        other.autoSettle(autoSettle);

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

    /**
     * @return the true derived type instance for use in this class.
     */
    protected abstract T self();

}
