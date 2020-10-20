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
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.types.transport.Open;

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
    private boolean traceFrames;

    private BiConsumer<Connection, ConnectionEvent> connectedhedHandler;
    private BiConsumer<Connection, ConnectionEvent> failedHandler;

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
        other.traceFrames(traceFrames);
        other.connectedHandler(connectedhedHandler);
        other.failedHandler(failedHandler);

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

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is opened.
     */
    public long openTimeout() {
        return openTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a resource such as a {@link Sender} or {@link Receiver} has been honored.
     *
     * @param openTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions openTimeout(long openTimeout) {
        this.openTimeout = openTimeout;
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is message send.
     */
    public long sendTimeout() {
        return sendTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote for settlement of a
     * message send from a {@link Sender} resource.  If the remote does not respond within the
     * configured timeout the {@link Tracker} associated with the sent message will reflect a
     * failed send.
     *
     * @param sendTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions sendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
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
     * return an error to the request initiator.
     *
     * @param requestTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions requestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
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
     * @return the configured idle timeout value that will be sent to the remote.
     */
    public long idleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets the idle timeout (in milliseconds) after which the connection will
     * be closed if the peer has not send any data. The provided value will be
     * halved before being transmitted as our advertised idle-timeout in the
     * AMQP {@link Open} frame.
     *
     * @param idleTimeout the timeout in milliseconds.
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions idleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    /**
     * @return the configured drain timeout value that will use to fail a pending drain request.
     */
    public long drainTimeout() {
        return drainTimeout;
    }

    /**
     * Sets the drain timeout (in milliseconds) after which a {@link Receiver} will be
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

    /**
     * @return the connection failed handler currently registered.
     */
    public BiConsumer<Connection, ConnectionEvent> failedHandler() {
        return failedHandler;
    }

    /**
     * Configures a handler that will be notified when the connection has failed and cannot be recovered
     * should reconnect be enabled.  Once notified of the failure the {@link Connection} is no longer
     * operable and the {@link Connection} APIs will throw an exception to indicate that the connection
     * has failed.  The client application should close a failed {@link Connection} once it becomes
     * aware of the failure to ensure all connection resources are cleaned up properly.
     *
     * @param failedHandler
     *      the connection failed handler to notify when the connection fails for any reason.
     *
     * @return this {@link ReconnectOptions} instance.
     *
     * @see #connectedHandler()
     */
    public ConnectionOptions failedHandler(BiConsumer<Connection, ConnectionEvent> failedHandler) {
        this.failedHandler = failedHandler;
        return this;
    }

    /**
     * @return the connection established handler that is currently registered
     */
    public BiConsumer<Connection, ConnectionEvent> connectedHandler() {
        return connectedhedHandler;
    }

    /**
     * Configures a handler that will be notified when a {@link Connection} has established.
     * This handler is called for each connection event when reconnection is enabled unless a
     * {@link #reconnectedHandler} is configured in which case this handler is only notified
     * on the first connection to a remote.
     *
     * @param connectedHandler
     *      the connection established handler to assign to these {@link ConnectionOptions}.
     *
     * @return this {@link ReconnectOptions} instance.
     *
     * @see #failedHandler()
     */
    public ConnectionOptions connectedHandler(BiConsumer<Connection, ConnectionEvent> connectedHandler) {
        this.connectedhedHandler = connectedHandler;
        return this;
    }
}
