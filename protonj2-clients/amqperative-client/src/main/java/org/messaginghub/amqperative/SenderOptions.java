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

    public SenderOptions setLinkName(String linkName) {
        this.linkName = linkName;
        return this;
    }

    public String getLinkName() {
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
    public SenderOptions setAutoSettle(boolean autoSettle) {
        this.autoSettle = autoSettle;
        return this;
    }

    /**
     * Get whether the receiver is auto settling deliveries.
     *
     * @return whether deliveries should be auto settled locally after being settled
     *         by the receiver
     * @see #setAutoSettle(boolean)
     */
    public boolean isAutoSettle() {
        return autoSettle;
    }

    public long getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public long getOpenTimeout() {
        return openTimeout;
    }

    public void setOpenTimeout(long openTimeout) {
        this.openTimeout = openTimeout;
    }

    public long getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public long getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /**
     * @return the offeredCapabilities
     */
    public String[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    /**
     * @param offeredCapabilities the offeredCapabilities to set
     */
    public void setOfferedCapabilities(String[] offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
    }

    /**
     * @return the desiredCapabilities
     */
    public String[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * @param desiredCapabilities the desiredCapabilities to set
     */
    public void setDesiredCapabilities(String[] desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
    }

    /**
     * @return the properties
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * @param properties the properties to set
     */
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
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
        other.setAutoSettle(autoSettle);
        other.setLinkName(linkName);
        other.setCloseTimeout(closeTimeout);
        other.setOpenTimeout(openTimeout);
        other.setSendTimeout(sendTimeout);
        other.setRequestTimeout(requestTimeout);

        if (offeredCapabilities != null) {
            other.setOfferedCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (desiredCapabilities != null) {
            other.setDesiredCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (properties != null) {
            other.setProperties(new HashMap<>(properties));
        }

        return this;
    }
}
