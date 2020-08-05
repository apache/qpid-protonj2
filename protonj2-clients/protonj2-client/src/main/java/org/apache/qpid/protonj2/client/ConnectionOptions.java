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
 * Options that control the behaviour of the {@link Connection} created from them.
 */
public class ConnectionOptions {

    public static final String[] DEFAULT_DESIRED_CAPABILITIES = new String[] { "ANONYMOUS-RELAY" };

    public static final long INFINITE = -1;
    public static final long DEFAULT_OPEN_TIMEOUT = 15000;
    public static final long DEFAULT_CLOSE_TIMEOUT = 60000;
    public static final long DEFAULT_SEND_TIMEOUT = INFINITE;
    public static final long DEFAULT_REQUEST_TIMEOUT = INFINITE;
    public static final long DEFAULT_IDLE_TIMEOUT = 60000;
    public static final long DEFAULT_DRAIN_TIMEOUT = 60000;
    public static final int DEFAULT_CHANNEL_MAX = 65535;
    public static final int DEFAULT_MAX_FRAME_SIZE = 65535;
    public static final boolean DEFAULT_ALLOW_INSECURE_REDIRECTS = false;

    private long sendTimeout = DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private long drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    private final TransportOptions transport = new TransportOptions();
    private final SslOptions ssl = new SslOptions();
    private final SaslOptions sasl = new SaslOptions();

    private String user;
    private String password;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private String[] offeredCapabilities;
    private String[] desiredCapabilities = DEFAULT_DESIRED_CAPABILITIES;
    private Map<String, Object> properties;
    private String virtualHost;
    private boolean allowInsecureRedirects = DEFAULT_ALLOW_INSECURE_REDIRECTS;
    private boolean traceFrames;

    /**
     * Create a new {@link ConnectionOptions} instance configured with default configuration settings.
     */
    public ConnectionOptions() {
        // Defaults
    }

    /**
     * Creates a {@link ConnectionOptions} instance that is a copy of the given instance.
     *
     * @param options
     *      The {@link ConnectionOptions} instance whose configuration should be copied to this one.
     */
    public ConnectionOptions(ConnectionOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link ConnectionOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    protected ConnectionOptions copyInto(ConnectionOptions other) {
        other.closeTimeout(closeTimeout);
        other.openTimeout(openTimeout);
        other.sendTimeout(sendTimeout);
        other.requestTimeout(requestTimeout);
        other.idleTimeout(idleTimeout);
        other.drainTimeout(drainTimeout);
        other.channelMax(channelMax);
        other.maxFrameSize(maxFrameSize);
        other.user(user);
        other.password(password);
        other.allowInsecureRedirects(allowInsecureRedirects);
        other.traceFrames(traceFrames);

        if (offeredCapabilities != null) {
            other.offeredCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (desiredCapabilities != null) {
            other.desiredCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (properties != null) {
            other.properties(new HashMap<>(properties));
        }

        transport.copyInto(other.transportOptions());
        ssl.copyInto(other.sslOptions());
        sasl.copyInto(other.saslOptions());

        return this;
    }

    // TODO - Proper Javadocs for the various configuration options

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is closed.
     */
    public long closeTimeout() {
        return closeTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * a resource such as a {@link Sender} or {@link Receiver} has been honored.
     *
     * @param closeTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions closeTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
        return this;
    }

    public long openTimeout() {
        return openTimeout;
    }

    public ConnectionOptions openTimeout(long openTimeout) {
        this.openTimeout = openTimeout;
        return this;
    }

    public long sendTimeout() {
        return sendTimeout;
    }

    public ConnectionOptions sendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
        return this;
    }

    public long requestTimeout() {
        return requestTimeout;
    }

    public ConnectionOptions requestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public long idleTimeout() {
        return idleTimeout;
    }

    public int channelMax() {
        return channelMax;
    }

    public ConnectionOptions channelMax(int channelMax) {
        this.channelMax = channelMax;
        return this;
    }

    public int maxFrameSize() {
        return maxFrameSize;
    }

    /**
     * Sets the max frame size (in bytes).
     *
     * Values of -1 indicates to use the proton default.
     *
     * @param maxFrameSize the frame size in bytes.
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions maxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    /**
     * Sets the idle timeout (in milliseconds) after which the connection will
     * be closed if the peer has not send any data. The provided value will be
     * halved before being transmitted as our advertised idle-timeout in the
     * AMQP Open frame.
     *
     * @param idleTimeout the timeout in milliseconds.
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions idleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public long drainTimeout() {
        return drainTimeout;
    }

    /**
     * Sets the drain timeout (in milliseconds) after which a receiver will be
     * treated as having failed and will be closed due to unknown state of the
     * remote having not responded to the requested drain.
     *
     * @param drainTimeout
     *      the drainTimeout to use for receiver links.
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions drainTimeout(long drainTimeout) {
        this.drainTimeout = drainTimeout;
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
     * @return this options object for chaining.
     */
    public ConnectionOptions offeredCapabilities(String... offeredCapabilities) {
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
     * @return this options object for chaining.
     */
    public ConnectionOptions desiredCapabilities(String... desiredCapabilities) {
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
     * @return this options object for chaining.
     */
    public ConnectionOptions properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return the virtual host value configured.
     */
    public String virtualHost() {
        return virtualHost;
    }

    /**
     * @param virtualHost
     * 		the virtual host to set
     *
     * @return this options instance.
     */
    public ConnectionOptions virtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    /**
     * @return the user
     */
    public String user() {
        return user;
    }

    /**
     * @param user the user to set
     *
     * @return this options instance.
     */
    public ConnectionOptions user(String user) {
        this.user = user;
        return this;
    }

    /**
     * @return the password
     */
    public String password() {
        return password;
    }

    /**
     * @param password the password to set
     *
     * @return this options instance.
     */
    public ConnectionOptions password(String password) {
        this.password = password;
        return this;
    }

    /**
     * @return the transport options that will be used for the {@link Connection}.
     */
    public TransportOptions transportOptions() {
        return transport;
    }

    /**
     * @return the SSL options that will be used for the {@link Connection}.
     */
    public SslOptions sslOptions() {
        return ssl;
    }

    /**
     * @return the SASL options that will be used for the {@link Connection}.
     */
    public SaslOptions saslOptions() {
        return sasl;
    }

    /**
     * @return the allowInsecureRedirects
     */
    public boolean allowInsecureRedirects() {
        return allowInsecureRedirects;
    }

    /**
     * When the {@link Connection} is remotely closed with an error containing redirect information
     * and that redirect would result in a connection being made using an insecure mechanism should
     * that redirection be followed or disregarded.  For example a secure Web Socket connection could
     * be redirected to and instructed to use a insecure Web Socket variant instead, this value controls
     * whether that would be followed or ignored.
     *
     * @param allowInsecureRedirects the allowInsecureRedirects to set
     *
     * @return this options instance.
     */
    public ConnectionOptions allowInsecureRedirects(boolean allowInsecureRedirects) {
        this.allowInsecureRedirects = allowInsecureRedirects;
        return this;
    }

    /**
     * Configure if the newly created connection should enabled AMQP frame tracing to the
     * system output.
     *
     * @param traceFrames
     * 		true if frame tracing on this connection should be enabled.
     *
     * @return this options instance.
     */
    public ConnectionOptions traceFrames(boolean traceFrames) {
        this.traceFrames = traceFrames;
        return this;
    }

    /**
     * @return true if the connection is configured to perform frame tracing.
     */
    public boolean traceFrames() {
        return this.traceFrames;
    }

    /**
     * @return true if SSL support has been enabled for this connection.
     */
    public boolean sslEnabled() {
        return ssl.sslEnabled();
    }

    /**
     * Controls if the connection will attempt to connect using a secure IO layer or not.
     * <p>
     * This option enables or disables SSL encryption when connecting to a remote peer.  To
     * control specifics of the SSL configuration for the {@link Connection} the values must
     * be updated in the {@link SslOptions} configuration prior to creating the connection.
     *
     * @param sslEnabled
     * 		Is SSL encryption enabled for the {@link Connection}.
     *
     * @return this options instance.
     */
    public ConnectionOptions sslEnabled(boolean sslEnabled) {
        ssl.sslEnabled(sslEnabled);
        return this;
    }
}
