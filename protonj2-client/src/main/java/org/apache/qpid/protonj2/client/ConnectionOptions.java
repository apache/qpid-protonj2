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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.types.transport.Open;

/**
 * Options that control the behavior of the {@link Connection} created from them.
 */
public class ConnectionOptions implements Cloneable {

    /**
     * Default value for the AMQP desired capabilities set in the Open frame.
     */
    private static final String[] DEFAULT_DESIRED_CAPABILITIES_ARRAY = new String[] { "ANONYMOUS-RELAY" };

    public static final List<String> DEFAULT_DESIRED_CAPABILITIES =
        Collections.unmodifiableList(Arrays.asList( DEFAULT_DESIRED_CAPABILITIES_ARRAY ));

    public static final long INFINITE = -1;
    public static final long DEFAULT_OPEN_TIMEOUT = 15000;
    public static final long DEFAULT_CLOSE_TIMEOUT = 60000;
    public static final long DEFAULT_SEND_TIMEOUT = INFINITE;
    public static final long DEFAULT_REQUEST_TIMEOUT = INFINITE;
    public static final long DEFAULT_IDLE_TIMEOUT = 60000;
    public static final long DEFAULT_DRAIN_TIMEOUT = 60000;
    public static final int DEFAULT_CHANNEL_MAX = 65535;
    public static final int DEFAULT_MAX_FRAME_SIZE = 65536;
    public static final NextReceiverPolicy DEFAULT_NEXT_RECEIVER_POLICY = NextReceiverPolicy.ROUND_ROBIN;

    private long sendTimeout = DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private long drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    private final TransportOptions transport = new TransportOptions();
    private final ReconnectOptions reconnect = new ReconnectOptions();
    private final SslOptions ssl = new SslOptions();
    private final SaslOptions sasl = new SaslOptions();

    private String user;
    private String password;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private String[] offeredCapabilities;
    private String[] desiredCapabilities = DEFAULT_DESIRED_CAPABILITIES_ARRAY;
    private Map<String, Object> properties;
    private String virtualHost;
    private boolean traceFrames;
    private NextReceiverPolicy nextReceiverPolicy = DEFAULT_NEXT_RECEIVER_POLICY;

    private BiConsumer<Connection, ConnectionEvent> connectedHandler;
    private BiConsumer<Connection, DisconnectionEvent> disconnectedHandler;
    private BiConsumer<Connection, DisconnectionEvent> interruptedHandler;
    private BiConsumer<Connection, ConnectionEvent> reconnectedHandler;

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

    @Override
    public ConnectionOptions clone() {
        return copyInto(new ConnectionOptions());
    }

    /**
     * Copy all options from this {@link ConnectionOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return the {@link ConnectionOptions} instance that was given.
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
        other.connectedHandler(connectedHandler);
        other.interruptedHandler(interruptedHandler);
        other.reconnectedHandler(reconnectedHandler);
        other.disconnectedHandler(disconnectedHandler);
        other.defaultNextReceiverPolicy(nextReceiverPolicy);
        other.virtualHost(virtualHost);

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
        reconnect.copyInto(other.reconnectOptions());

        return other;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is closed.
     */
    public long closeTimeout() {
        return closeTimeout;
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * a resource such as a {@link Connection}, {@link Session}, {@link Sender} or {@link Receiver} h
     * as been honored.
     *
     * @param closeTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions closeTimeout(long closeTimeout) {
        return closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * a resource such as a {@link Connection}, {@link Session}, {@link Sender} or {@link Receiver} h
     * as been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions closeTimeout(long timeout, TimeUnit units) {
        this.closeTimeout = units.toMillis(timeout);
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
     * a resource such as a {@link Connection}, {@link Session}, {@link Sender} or {@link Receiver}
     * has been honored.
     *
     * @param openTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions openTimeout(long openTimeout) {
        return openTimeout(openTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a resource such as a {@link Connection}, {@link Session}, {@link Sender} or {@link Receiver}
     * has been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions openTimeout(long timeout, TimeUnit units) {
        this.openTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is message send.
     */
    public long sendTimeout() {
        return sendTimeout;
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param sendTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions sendTimeout(long sendTimeout) {
        return sendTimeout(sendTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions sendTimeout(long timeout, TimeUnit units) {
        this.sendTimeout = units.toMillis(timeout);
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
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions requestTimeout(long requestTimeout) {
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
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions requestTimeout(long timeout, TimeUnit units) {
        this.requestTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the configured or default channel max value for create {@link Connection} instances.
     */
    public int channelMax() {
        return channelMax;
    }

    /**
     * Configure the channel maximum value for the new {@link Connection} created with these options.
     * <p>
     * The channel max value controls how many {@link Session} instances can be created by a given
     * Connection, the default value is <i>65535</i>.
     *
     * @param channelMax
     *      The channel max value to assign to newly created {@link Connection} instances.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions channelMax(int channelMax) {
        if (channelMax < 0 || channelMax > 65535) {
            throw new IllegalArgumentException("Cannot set a channel max less than zero or greater than 65535");
        }

        this.channelMax = channelMax;
        return this;
    }

    /**
     * @return the configure maximum frame size value for newly create {@link Connection} instances.
     */
    public int maxFrameSize() {
        return maxFrameSize;
    }

    /**
     * Sets the max frame size (in bytes), values of -1 indicates to use the client selected default.
     *
     * @param maxFrameSize the frame size in bytes.
     *
     * @return this {@link ConnectionOptions} instance.
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
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions idleTimeout(long idleTimeout) {
        return idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the idle timeout value after which the connection will be closed
     * if the peer has not send any data. The provided value will be halved before
     * being transmitted as our advertised idle-timeout in the AMQP {@link Open} frame.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions idleTimeout(long timeout, TimeUnit units) {
        this.idleTimeout = units.toMillis(timeout);
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
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions drainTimeout(long drainTimeout) {
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
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions drainTimeout(long timeout, TimeUnit units) {
        this.drainTimeout = units.toMillis(timeout);
        return this;
    }

    /**
     * @return the offeredCapabilities that have been configured.
     */
    public String[] offeredCapabilities() {
        return offeredCapabilities;
    }

    /**
     * Sets the collection of capabilities to offer to the remote from a new {@link Connection}
     * created using these {@link ConnectionOptions}.  The offered capabilities advertise to the
     * remote capabilities that this {@link Connection} supports.
     *
     * @param offeredCapabilities
     *      the offeredCapabilities to set on a new {@link Connection}.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions offeredCapabilities(String... offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
        return this;
    }

    /**
     * @return the desiredCapabilities that have been configured.
     */
    public String[] desiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * Sets the collection of capabilities to request from the remote for a new {@link Connection}
     * created using these {@link ConnectionOptions}.  The desired capabilities inform the remote
     * peer of the various capabilities the new {@link Connection} requires and the remote should
     * return those that it supports in its offered capabilities.
     *
     * @param desiredCapabilities
     *      the desiredCapabilities to set on a new {@link Connection}.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions desiredCapabilities(String... desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
        return this;
    }

    /**
     * @return the properties that have been configured.
     */
    public Map<String, Object> properties() {
        return properties;
    }

    /**
     * Sets a {@link Map} of properties to convey to the remote when a new {@link Connection}
     * is created from these {@link ConnectionOptions}.
     *
     * @param properties the properties to set
     *
     * @return this {@link ConnectionOptions} instance.
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
     * The virtual host value to provide to the remote when creating a new {@link Connection}.
     *
     * @param virtualHost
     * 		the virtual host to set
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions virtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    /**
     * @return the user name that is configured for new {@link Connection} instances.
     */
    public String user() {
        return user;
    }

    /**
     * Sets the user name used when performing connection authentication.
     *
     * @param user the user to set
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions user(String user) {
        this.user = user;
        return this;
    }

    /**
     * @return the password that is configured for new {@link Connection} instances.
     */
    public String password() {
        return password;
    }

    /**
     * Sets the password used when performing connection authentication.
     *
     * @param password the password to set
     *
     * @return this {@link ConnectionOptions} instance.
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
     * @return true if reconnection support has been enabled for this connection.
     */
    public boolean reconnectEnabled() {
        return reconnect.reconnectEnabled();
    }

    /**
     * Controls if the connection will attempt to reconnect if unable to connect immediately
     * or if an existing connection fails (default is disabled).
     * <p>
     * This option enables or disables reconnection to a remote peer after IO errors or remote
     * forcibly closing the connection. To control specifics of the reconnection configuration
     * for the {@link Connection} the values must be updated in the {@link ReconnectOptions}
     * configuration prior to creating the connection.
     *
     * @param reconnectEnabled
     *      Controls if reconnection is enabled or not for the associated {@link Connection}.
     *
     * @return this options instance.
     */
    public ConnectionOptions reconnectEnabled(boolean reconnectEnabled) {
        reconnect.reconnectEnabled(reconnectEnabled);
        return this;
    }

    /**
     * @return the reconnection options that will be used for the {@link Connection}.
     */
    public ReconnectOptions reconnectOptions() {
        return reconnect;
    }

    /**
     * Configure if the newly created connection should enabled AMQP frame tracing to the
     * system output.
     *
     * @param traceFrames
     * 		true if frame tracing on this connection should be enabled.
     *
     * @return this {@link ConnectionOptions} instance.
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
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions sslEnabled(boolean sslEnabled) {
        ssl.sslEnabled(sslEnabled);
        return this;
    }

    /**
     * @return the configured default next receiver policy for the connection.
     */
    public NextReceiverPolicy defaultNextReceiverPolicy() {
        return nextReceiverPolicy;
    }

    /**
     * Configures the default next receiver policy for this connection and any session
     * that is created without specifying user defined session default options.
     *
     * @param policy
     * 		The next receiver policy to assign as the default.
     *
     * @return this {@link ConnectionOptions} instance.
     */
    public ConnectionOptions defaultNextReceiverPolicy(NextReceiverPolicy policy) {
        this.nextReceiverPolicy = policy;
        return this;
    }

    /**
     * @return the connection failed handler currently registered.
     */
    public BiConsumer<Connection, DisconnectionEvent> disconnectedHandler() {
        return disconnectedHandler;
    }

    /**
     * Configures a handler that will be notified when the connection has failed and cannot be recovered
     * should reconnect be enabled.  Once notified of the failure the {@link Connection} is no longer
     * operable and the {@link Connection} APIs will throw an exception to indicate that the connection
     * has failed.  The client application should close a failed {@link Connection} once it becomes
     * aware of the failure to ensure all connection resources are cleaned up properly.
     *
     * @param disconnectedHandler
     *      the connection failed handler to notify when the connection fails for any reason.
     *
     * @return this {@link ConnectionOptions} instance.
     *
     * @see #interruptedHandler
     * @see #connectedHandler
     * @see #disconnectedHandler
     */
    public ConnectionOptions disconnectedHandler(BiConsumer<Connection, DisconnectionEvent> disconnectedHandler) {
        this.disconnectedHandler = disconnectedHandler;
        return this;
    }

    /**
     * @return the connection established handler that is currently registered
     */
    public BiConsumer<Connection, ConnectionEvent> connectedHandler() {
        return connectedHandler;
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
     * @return this {@link ConnectionOptions} instance.
     *
     * @see #disconnectedHandler()
     * @see #interruptedHandler
     * @see #reconnectedHandler
     */
    public ConnectionOptions connectedHandler(BiConsumer<Connection, ConnectionEvent> connectedHandler) {
        this.connectedHandler = connectedHandler;
        return this;
    }

    /**
     * @return the connection interrupted handler that is currently registered
     */
    public BiConsumer<Connection, DisconnectionEvent> interruptedHandler() {
        return interruptedHandler;
    }

    /**
     * Configures a handler that will be notified when the current {@link Connection} experiences an
     * interruption.  The {@link Connection} will only signal this handler when the reconnection feature
     * is enabled and will follow this event either with a notification that the connection has been
     * restored (if a handler is registered), or with a notification that the connection has failed
     * if the reconnection configuration places limits on the the number of reconnection attempts.
     *
     * @param interruptedHandler
     *      the connection interrupted handler to assign to these {@link ConnectionOptions}.
     *
     * @return this {@link ReconnectOptions} instance.
     *
     * @see #connectedHandler
     * @see #reconnectedHandler
     * @see #disconnectedHandler
     */
    public ConnectionOptions interruptedHandler(BiConsumer<Connection, DisconnectionEvent> interruptedHandler) {
        this.interruptedHandler = interruptedHandler;
        return this;
    }

    /**
     * @return the connection restored handler that is currently registered
     */
    public BiConsumer<Connection, ConnectionEvent> reconnectedHandler() {
        return reconnectedHandler;
    }

    /**
     * Configures a handler that will be notified when a {@link Connection} that has previously
     * experienced and interruption has been reconnected to a remote based on the reconnection
     * configuration.
     *
     * @param reconnectedHandler
     *      the connection restored handler to assign to these {@link ConnectionOptions}.
     *
     * @return this {@link ReconnectOptions} instance.
     *
     * @see #connectedHandler
     * @see #interruptedHandler
     * @see #disconnectedHandler
     */
    public ConnectionOptions reconnectedHandler(BiConsumer<Connection, ConnectionEvent> reconnectedHandler) {
        this.reconnectedHandler = reconnectedHandler;
        return this;
    }
}
