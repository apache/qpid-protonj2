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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

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
    public static final boolean DEFAULT_SASL_ENABLED = true;

    private long sendTimeout = DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private long drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    private final TransportOptions transport = new TransportOptions();
    private final SslOptions ssl = new SslOptions();

    private String user;
    private String password;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private String[] offeredCapabilities;
    private String[] desiredCapabilities = DEFAULT_DESIRED_CAPABILITIES;
    private Map<String, Object> properties;
    private String vhost;
    private String futureType;
    private boolean saslEnabled = DEFAULT_SASL_ENABLED;
    private final Set<String> saslAllowedMechs = new LinkedHashSet<>();
    private boolean allowInsecureRedirects = DEFAULT_ALLOW_INSECURE_REDIRECTS;

    public ConnectionOptions() {
    }

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
     * @return this options class for chaining.
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
        other.futureType(futureType);
        other.user(user);
        other.password(password);
        other.saslEnabled(saslEnabled);
        other.saslAllowedMechs.addAll(this.saslAllowedMechs);
        other.allowInsecureRedirects(allowInsecureRedirects);

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

        return this;
    }

    // TODO - Proper Javadocs

    public long closeTimeout() {
        return closeTimeout;
    }

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
     * @return the configure future type to use for this client connection
     */
    public String futureType() {
        return futureType;
    }

    /**
     * Sets the desired future type that the client connection should use when creating
     * the futures used by the API.
     *
     * @param futureType
     *      The name of the future type to use.
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions futureType(String futureType) {
        this.futureType = futureType;
        return this;
    }

    /**
     * @return the vhost
     */
    public String vhost() {
        return vhost;
    }

    /**
     * @param vhost the vhost to set
     *
     * @return this options instance.
     */
    public ConnectionOptions vhost(String vhost) {
        this.vhost = vhost;
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
     * @return the allowInsecureRedirects
     */
    public boolean allowInsecureRedirects() {
        return allowInsecureRedirects;
    }

    /**
     * @param allowInsecureRedirects the allowInsecureRedirects to set
     *
     * @return this options instance.
     */
    public ConnectionOptions allowInsecureRedirects(boolean allowInsecureRedirects) {
        this.allowInsecureRedirects = allowInsecureRedirects;
        return this;
    }

    /**
     * @return the saslLayer
     */
    public boolean saslEnabled() {
        return saslEnabled;
    }

    /**
     * @param saslEnabled the saslLayer to set
     *
     * @return this options instance.
     */
    public ConnectionOptions saslEnabled(boolean saslEnabled) {
        this.saslEnabled = saslEnabled;
        return this;
    }

    /**
     * @return true if SSL support has been enabled for this connection.
     */
    public boolean sslEnabled() {
        return ssl.sslEnabled();
    }

    /**
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
     * Adds a mechanism to the list of allowed SASL mechanisms this client will use
     * when selecting from the remote peers offered set of SASL mechanisms.  If no
     * allowed mechanisms are configured then the client will select the first mechanism
     * from the server offered mechanisms that is supported.
     *
     * @param mechanism
     * 		The mechanism to allow.
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions addAllowedMechanism(String mechanism) {
        this.saslAllowedMechs.add(mechanism);
        return this;
    }

    /**
     * @return the current list of allowed SASL Mechanisms.
     */
    public Set<String> allowedMechanisms() {
        return Collections.unmodifiableSet(saslAllowedMechs);
    }
}
