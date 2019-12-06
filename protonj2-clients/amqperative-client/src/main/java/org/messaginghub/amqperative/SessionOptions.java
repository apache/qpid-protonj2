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
 * Options that control the behaviour of the {@link Session} created from them.
 */
public class SessionOptions {

    static final SessionOptions DEFAULT = new SessionOptions();

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

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

    public void closeTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public long openTimeout() {
        return openTimeout;
    }

    public void openTimeout(long connectTimeout) {
        this.openTimeout = connectTimeout;
    }

    public long sendTimeout() {
        return sendTimeout;
    }

    public void sendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public long requestTimeout() {
        return requestTimeout;
    }

    public void requestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /**
     * @return the offeredCapabilities
     */
    public String[] offeredCapabilities() {
        return offeredCapabilities;
    }

    /**
     * @param offeredCapabilities the offeredCapabilities to set
     */
    public void offeredCapabilities(String... offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
    }

    /**
     * @return the desiredCapabilities
     */
    public String[] desiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * @param desiredCapabilities the desiredCapabilities to set
     */
    public void desiredCapabilities(String... desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
    }

    /**
     * @return the properties
     */
    public Map<String, Object> properties() {
        return properties;
    }

    /**
     * @param properties the properties to set
     */
    public void properties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
