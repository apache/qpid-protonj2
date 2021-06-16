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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Options that control the reconnection behavior of a client {@link Connection}.
 */
public class ReconnectOptions {

    public static final boolean DEFAULT_RECONNECT_ENABLED = false;
    public static final int INFINITE = -1;
    public static final int DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS = 10;
    public static final int DEFAULT_RECONNECT_DELAY = 10;
    public static final int DEFAULT_MAX_RECONNECT_DELAY = 30_000;
    public static final boolean DEFAULT_USE_RECONNECT_BACKOFF = true;
    public static final double DEFAULT_RECONNECT_BACKOFF_MULTIPLIER = 2.0d;

    private final List<ReconnectLocation> reconnectHosts = new ArrayList<>();

    private boolean reconnectEnabled = DEFAULT_RECONNECT_ENABLED;
    private int warnAfterReconnectAttempts = DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS;
    private int maxInitialConnectionAttempts = INFINITE;
    private int maxReconnectAttempts = INFINITE;
    private int reconnectDelay = DEFAULT_RECONNECT_DELAY;
    private int maxReconnectDelay = DEFAULT_RECONNECT_DELAY;
    private boolean useReconnectBackOff = DEFAULT_USE_RECONNECT_BACKOFF;
    private double reconnectBackOffMultiplier = DEFAULT_RECONNECT_BACKOFF_MULTIPLIER;

    /**
     * Create a new {@link ReconnectOptions} instance configured with default configuration settings.
     */
    public ReconnectOptions() {
    }

    /**
     * Creates a {@link ReconnectOptions} instance that is a copy of the given instance.
     *
     * @param options
     *      The {@link ReconnectOptions} instance whose configuration should be copied to this one.
     */
    public ReconnectOptions(ReconnectOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link ReconnectOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    protected ReconnectOptions copyInto(ReconnectOptions other) {
        other.reconnectEnabled(reconnectEnabled());
        other.warnAfterReconnectAttempts(warnAfterReconnectAttempts);
        other.maxInitialConnectionAttempts(maxInitialConnectionAttempts);
        other.maxReconnectAttempts(maxReconnectAttempts);
        other.reconnectDelay(reconnectDelay);
        other.maxReconnectDelay(maxReconnectDelay);
        other.useReconnectBackOff(useReconnectBackOff);
        other.reconnectBackOffMultiplier(reconnectBackOffMultiplier);
        other.reconnectHosts.addAll(reconnectHosts);

        return this;
    }

    /**
     * Returns <code>true</code> if reconnect is currently enabled for the {@link Connection} that
     * these options are assigned to.
     *
     * @return the reconnect enabled configuration state for this options instance.
     */
    public boolean reconnectEnabled() {
        return reconnectEnabled;
    }

    /**
     * Set to <code>true</code> to enable reconnection support on the associated {@link Connection}
     * or <code>false</code> to disable.  When enabled a {@link Connection} will attempt to reconnect
     * to a remote based on the configuration set in this options instance.
     *
     * @param reconnectEnabled
     *      Controls if reconnection is enabled or not for the associated {@link Connection}.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions reconnectEnabled(boolean reconnectEnabled) {
        this.reconnectEnabled = reconnectEnabled;
        return this;
    }

    /**
     * Adds an additional reconnection location that can be used when attempting to reconnect the client
     * following a connection failure.
     *
     * @param host
     *      The host name of the remote host to attempt a reconnection to.
     * @param port
     *      The port on the remote host to use when connecting.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions addReconnectLocation(String host, int port) {
        reconnectHosts.add(new ReconnectLocation(host, port));
        return this;
    }

    /**
     * @return an unmodifiable view of the configured reconnect locations.
     */
    public List<ReconnectLocation> reconnectLocations() {
        return Collections.unmodifiableList(reconnectHosts);
    }

    /**
     * @return the number of reconnection attempt before the client should log a warning.
     */
    public int warnAfterReconnectAttempts() {
        return warnAfterReconnectAttempts;
    }

    /**
     * Controls how often the client will log a message indicating that a reconnection is being attempted.
     * The default is to log every 10 connection attempts.
     *
     * @param warnAfterReconnectAttempts
     *      The number of attempts before logging an update about not yet reconnecting.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions warnAfterReconnectAttempts(int warnAfterReconnectAttempts) {
        this.warnAfterReconnectAttempts = warnAfterReconnectAttempts;
        return this;
    }

    /**
     * @return the configured maximum number of initial connection attempts to try before giving up
     */
    public int maxInitialConnectionAttempts() {
        return maxInitialConnectionAttempts;
    }

    /**
     * For a client that has never connected to a remote peer before this option controls how many attempts
     * are made to connect before reporting the connection as failed. The default behavior is to use the
     * value of maxReconnectAttempts.
     *
     * @param maxInitialConnectionAttempts
     *      the maximum number of initial connection attempts to try before giving up.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions maxInitialConnectionAttempts(int maxInitialConnectionAttempts) {
        this.maxInitialConnectionAttempts = maxInitialConnectionAttempts;
        return this;
    }

    /**
     * @return the configured maximum number of reconnection attempts to try before giving up
     */
    public int maxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    /**
     * The number of reconnection attempts allowed before reporting the connection as failed to the client.
     * The default is no limit or (-1).
     *
     * @param maxReconnectionAttempts
     *      the maximum number of reconnection attempts to try before giving up.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions maxReconnectAttempts(int maxReconnectionAttempts) {
        this.maxReconnectAttempts = maxReconnectionAttempts;
        return this;
    }

    /**
     * @return the configured reconnect delay to use after between attempts to connect or reconnect.
     */
    public int reconnectDelay() {
        return reconnectDelay;
    }

    /**
     * Controls the delay between successive reconnection attempts, defaults to 10 milliseconds. If the
     * back off option is not enabled this value remains constant.
     *
     * @param reconnectDelay
     *      The reconnect delay to apply to successive attempts to reconnect.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions reconnectDelay(int reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
        return this;
    }

    /**
     * @return the configured maximum reconnect attempt delay allowed when using delay back off scheduling.
     */
    public int maxReconnectDelay() {
        return maxReconnectDelay;
    }

    /**
     * The maximum time that the client will wait before attempting a reconnect. This value is only used when the
     * back off feature is enabled to ensure that the delay does not grow too large. Defaults to 30 seconds as the
     * max time between successive connection attempts.
     *
     * @param maxReconnectDelay
     *      The maximum interval allowed when connection attempt back off is in effect.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions maxReconnectDelay(int maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
        return this;
    }

    /**
     * @return if the reconnection attempts will be delayed using a back off multiplier.
     */
    public boolean useReconnectBackOff() {
        return useReconnectBackOff;
    }

    /**
     * Controls whether the time between reconnection attempts should grow based on a configured multiplier.
     * This option defaults to true.
     *
     * @param useReconnectBackOff
     *      should connection attempts use a back off of the configured delay.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions useReconnectBackOff(boolean useReconnectBackOff) {
        this.useReconnectBackOff = useReconnectBackOff;
        return this;
    }

    /**
     * @return the multiplier used when the reconnection back off feature is enabled.
     */
    public double reconnectBackOffMultiplier() {
        return reconnectBackOffMultiplier;
    }

    /**
     * The multiplier used to grow the reconnection delay value, defaults to 2.0d.
     *
     * @param reconnectBackOffMultiplier
     *      the delay multiplier used when building delay between reconnection attempts.
     *
     * @return this {@link ReconnectOptions} instance.
     */
    public ReconnectOptions reconnectBackOffMultiplier(double reconnectBackOffMultiplier) {
        this.reconnectBackOffMultiplier = reconnectBackOffMultiplier;
        return this;
    }
}
