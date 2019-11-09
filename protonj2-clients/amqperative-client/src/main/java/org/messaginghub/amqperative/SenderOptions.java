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
package org.messaginghub.amqperative;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Options that control the behavior of a {@link Sender} created from them.
 */
public class SenderOptions {

    // TODO: simplify configuration options for things like durable subs, shared subs? Or add a helper to create the options?

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private String linkName;
    private boolean autoSettle;

    private final SourceOptions source = new SourceOptions();
    private final TargetOptions target = new TargetOptions();

    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;

    public SenderOptions() {
    }

    public SenderOptions(SenderOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    public SenderOptions linkName(String linkName) {
        this.linkName = linkName;
        return this;
    }

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
     * @return the sender
     */
    public SenderOptions autoSettle(boolean autoSettle) {
        this.autoSettle = autoSettle;
        return this;
    }

    /**
     * Get whether the receiver is auto settling deliveries.
     *
     * @return whether deliveries should be auto settled locally after being settled
     *         by the receiver
     * @see #autoSettle(boolean)
     */
    public boolean autoSettle() {
        return autoSettle;
    }

    public long closeTimeout() {
        return closeTimeout;
    }

    public SenderOptions closeTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
        return this;
    }

    public long openTimeout() {
        return openTimeout;
    }

    public SenderOptions openTimeout(long openTimeout) {
        this.openTimeout = openTimeout;
        return this;
    }

    public long sendTimeout() {
        return sendTimeout;
    }

    public SenderOptions sendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
        return this;
    }

    public long requestTimeout() {
        return requestTimeout;
    }

    public SenderOptions requestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
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
    public SenderOptions offeredCapabilities(String[] offeredCapabilities) {
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
    public SenderOptions desiredCapabilities(String[] desiredCapabilities) {
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
        // TODO - Copy source and target options
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

        return this;
    }
}
