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
 * Options that control the behavior of the {@link Receiver} created from them.
 */
public class ReceiverOptions {

    // TODO: simplify configuration options for things like durable subs, shared subs? Or add a helper to create the options?

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private boolean autoAccept = true;
    private DeliveryMode deliveryMode = DeliveryMode.AT_LEAST_ONCE;
    private int creditWindow = 10;
    private String linkName;

    private final SourceOptions source = new SourceOptions();
    private final TargetOptions target = new TargetOptions();

    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;

    public ReceiverOptions() {
    }

    public ReceiverOptions(ReceiverOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    public ReceiverOptions autoAccept(boolean autoAccept) {
        this.autoAccept = autoAccept;
        return this;
    }

    public boolean autoAccept() {
        return autoAccept;
    }

    public ReceiverOptions deliveryMode(DeliveryMode deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    public DeliveryMode deliveryMode() {
        return deliveryMode;
    }

    public ReceiverOptions linkName(String linkName) {
        this.linkName = linkName;
        return this;
    }

    public String linkName() {
        return linkName;
    }

    public int creditWindow() {
        return creditWindow;
    }

    public ReceiverOptions creditWindow(int creditWindow) {
        this.creditWindow = creditWindow;
        return this;
    }

    public long closeTimeout() {
        return closeTimeout;
    }

    public ReceiverOptions closeTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
        return this;
    }

    public long openTimeout() {
        return openTimeout;
    }

    public ReceiverOptions openTimeout(long openTimeout) {
        this.openTimeout = openTimeout;
        return this;
    }

    public long sendTimeout() {
        return sendTimeout;
    }

    public ReceiverOptions sendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
        return this;
    }

    public long requestTimeout() {
        return requestTimeout;
    }

    public ReceiverOptions requestTimeout(long requestTimeout) {
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
