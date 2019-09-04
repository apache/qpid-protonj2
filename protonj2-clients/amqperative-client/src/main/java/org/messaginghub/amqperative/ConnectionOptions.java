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
 * Options that control the behaviour of the {@link Connection} created from them.
 */
public class ConnectionOptions {

    public static final long INFINITE = -1;
    public static final long DEFAULT_CONNECT_TIMEOUT = 15000;
    public static final long DEFAULT_CLOSE_TIMEOUT = 60000;
    public static final long DEFAULT_SEND_TIMEOUT = INFINITE;
    public static final long DEFAULT_REQUEST_TIMEOUT = INFINITE;
    public static final long DEFAULT_IDLE_TIMEOUT = 60000;
    public static final long DEFAULT_DRAIN_TIMEOUT = 60000;
    public static final int DEFAULT_CHANNEL_MAX = 65535;
    public static final int DEFAULT_MAX_FRAME_SIZE = 65535;
    public static final boolean DEFAULT_ALLOW_INSECURE_REDIRECTS = false;
    public static final boolean DEFAULT_SASL_ENABLED = true;
    public static final boolean DEFAULT_SASL_ALLOW_INSECURE_MECHS = false;

    private long sendTimeout = DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private long drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    private final TransportOptions transport = new TransportOptions();

    private String user;
    private String password;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;
    private String vhost;
    private String futureType;
    private boolean saslEnabled = DEFAULT_SASL_ENABLED;
    private boolean saslAllowInsecureMechs = DEFAULT_SASL_ALLOW_INSECURE_MECHS;
    private String saslAllowedMechanisms;
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
        other.setCloseTimeout(closeTimeout);
        other.setConnectTimeout(connectTimeout);
        other.setSendTimeout(sendTimeout);
        other.setRequestTimeout(requestTimeout);
        other.setIdleTimeout(idleTimeout);
        other.setDrainTimeout(drainTimeout);
        other.setChannelMax(channelMax);
        other.setMaxFrameSize(maxFrameSize);
        other.setFutureType(futureType);
        other.setUser(user);
        other.setPassword(password);
        other.setSaslEnabled(saslEnabled);
        other.setSaslAllowInsecureMechs(saslAllowInsecureMechs);
        other.setSaslAllowedMechanisms(saslAllowedMechanisms);
        other.setAllowInsecureRedirects(allowInsecureRedirects);

        if (offeredCapabilities != null) {
            other.setOfferedCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (desiredCapabilities != null) {
            other.setDesiredCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (properties != null) {
            other.setProperties(new HashMap<>(properties));
        }

        transport.copyInto(other.getTransport());

        return this;
    }

    // TODO - Proper Javadocs

    public long getCloseTimeout() {
        return closeTimeout;
    }

    public ConnectionOptions setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
        return this;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public ConnectionOptions setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public long getSendTimeout() {
        return sendTimeout;
    }

    public ConnectionOptions setSendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
        return this;
    }

    public long getRequestTimeout() {
        return requestTimeout;
    }

    public ConnectionOptions setRequestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public ConnectionOptions setChannelMax(int channelMax) {
        this.channelMax = channelMax;
        return this;
    }

    public int getMaxFrameSize() {
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
    public ConnectionOptions setMaxFrameSize(int maxFrameSize) {
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
    public ConnectionOptions setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public long getDrainTimeout() {
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
    public ConnectionOptions setDrainTimeout(long drainTimeout) {
        this.drainTimeout = drainTimeout;
        return this;
    }

    /**
     * @return the offeredCapabilities
     */
    public String[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    /**
     * @param offeredCapabilities the offeredCapabilities to set
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions setOfferedCapabilities(String[] offeredCapabilities) {
        this.offeredCapabilities = offeredCapabilities;
        return this;
    }

    /**
     * @return the desiredCapabilities
     */
    public String[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    /**
     * @param desiredCapabilities the desiredCapabilities to set
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions setDesiredCapabilities(String[] desiredCapabilities) {
        this.desiredCapabilities = desiredCapabilities;
        return this;
    }

    /**
     * @return the properties
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * @param properties the properties to set
     *
     * @return this options object for chaining.
     */
    public ConnectionOptions setProperties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return the configure future type to use for this client connection
     */
    public String getFutureType() {
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
    public ConnectionOptions setFutureType(String futureType) {
        this.futureType = futureType;
        return this;
    }

    /**
     * @return the vhost
     */
    public String getVhost() {
        return vhost;
    }

    /**
     * @param vhost the vhost to set
     */
    public void setVhost(String vhost) {
        this.vhost = vhost;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the transport options that will be used for the {@link Connection}.
     */
    public TransportOptions getTransport() {
        return transport;
    }

    /**
     * @return the allowInsecureRedirects
     */
    public boolean isAllowInsecureRedirects() {
        return allowInsecureRedirects;
    }

    /**
     * @param allowInsecureRedirects the allowInsecureRedirects to set
     */
    public void setAllowInsecureRedirects(boolean allowInsecureRedirects) {
        this.allowInsecureRedirects = allowInsecureRedirects;
    }

    /**
     * @return the saslLayer
     */
    public boolean isSaslEnabled() {
        return saslEnabled;
    }

    /**
     * @param saslEnabled the saslLayer to set
     */
    public void setSaslEnabled(boolean saslEnabled) {
        this.saslEnabled = saslEnabled;
    }

    /**
     * @return the saslMechanisms
     */
    public String getSaslAllowedMechanisms() {
        return saslAllowedMechanisms;
    }

    /**
     * @param saslAllowedMechanisms the SASL Mechanisms to allow
     */
    public void setSaslAllowedMechanisms(String saslAllowedMechanisms) {
        this.saslAllowedMechanisms = saslAllowedMechanisms;
    }

    /**
     * @return the saslAllowInsecureMechs
     */
    public boolean isSaslAllowInsecureMechs() {
        return saslAllowInsecureMechs;
    }

    /**
     * @param saslAllowInsecureMechs the saslAllowInsecureMechs to set
     */
    public void setSaslAllowInsecureMechs(boolean saslAllowInsecureMechs) {
        this.saslAllowInsecureMechs = saslAllowInsecureMechs;
    }
}
