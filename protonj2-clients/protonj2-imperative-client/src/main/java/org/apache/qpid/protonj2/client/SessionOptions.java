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

/**
 * Options that control the behaviour of the {@link Session} created from them.
 */
public class SessionOptions {

    public static final int DEFAULT_SESSION_CAPACITY = 100 * 1024 * 1024;

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private int incomingCapacity = DEFAULT_SESSION_CAPACITY;
    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;

    public SessionOptions() {
    }

    public SessionOptions(SessionOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link SessionOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    protected SessionOptions copyInto(SessionOptions other) {
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

    public long closeTimeout() {
        return closeTimeout;
    }

    public SessionOptions closeTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
        return this;
    }

    public long openTimeout() {
        return openTimeout;
    }

    public SessionOptions openTimeout(long connectTimeout) {
        this.openTimeout = connectTimeout;
        return this;
    }

    public long sendTimeout() {
        return sendTimeout;
    }

    public SessionOptions sendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
        return this;
    }

    public long requestTimeout() {
        return requestTimeout;
    }

    public SessionOptions requestTimeout(long requestTimeout) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions offeredCapabilities(String... offeredCapabilities) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions desiredCapabilities(String... desiredCapabilities) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return the incoming capacity that is configured for newly created {@link Session} instances.
     */
    public int incomingCapacity() {
        return incomingCapacity;
    }

    /**
     * Sets the incoming capacity for a {@link Session} the created session.  The incoming capacity
     * control how much buffering a session will allow before applying back pressure to the remote
     * thereby preventing excessive memory overhead.
     * <p>
     * This is an advanced option and in most cases the client defaults should be left in place unless
     * a specific issue needs to be addressed.
     *
     * @param incomingCapacity
     *      the incoming capacity to set when creating a new {@link Session}.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions incomingCapacity(int incomingCapacity) {
        this.incomingCapacity = incomingCapacity;
        return this;
    }
}
