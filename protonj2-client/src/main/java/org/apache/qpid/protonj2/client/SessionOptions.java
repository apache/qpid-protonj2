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
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;

/**
 * Options that control the behavior of the {@link Session} created from them.
 */
public class SessionOptions implements Cloneable {

    /**
     * The default Session configured incoming capacity limit to provide to the remote
     */
    public static final int DEFAULT_SESSION_INCOMING_CAPACITY = 100 * 1024 * 1024;

    /**
     * The default Session configured outgoing capacity to apply to local pending writes.
     */
    public static final int DEFAULT_SESSION_OUTGOING_CAPACITY = 100 * 1024 * 1024;

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;
    private long drainTimeout = ConnectionOptions.DEFAULT_DRAIN_TIMEOUT;
    private long requestTimeout = ConnectionOptions.DEFAULT_REQUEST_TIMEOUT;
    private long openTimeout = ConnectionOptions.DEFAULT_OPEN_TIMEOUT;
    private long closeTimeout = ConnectionOptions.DEFAULT_CLOSE_TIMEOUT;

    private int incomingCapacity = DEFAULT_SESSION_INCOMING_CAPACITY;
    private int outgoingCapacity = DEFAULT_SESSION_OUTGOING_CAPACITY;

    private String[] offeredCapabilities;
    private String[] desiredCapabilities;
    private Map<String, Object> properties;
    private NextReceiverPolicy nextReceiverPolicy = ConnectionOptions.DEFAULT_NEXT_RECEIVER_POLICY;

    /**
     * Create a new {@link SessionOptions} instance configured with default configuration settings.
     */
    public SessionOptions() {
    }

    /**
     * Create a new SessionOptions instance that copies the configuration from the specified source options.
     *
     * @param options
     * 		The SessionOptions instance whose settings are to be copied into this one.
     */
    public SessionOptions(SessionOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    @Override
    public SessionOptions clone() {
        return copyInto(new SessionOptions());
    }

    /**
     * Copy all options from this {@link SessionOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return the {@link SessionOptions} instance that was given.
     */
    protected SessionOptions copyInto(SessionOptions other) {
        other.closeTimeout(closeTimeout);
        other.openTimeout(openTimeout);
        other.sendTimeout(sendTimeout);
        other.drainTimeout(drainTimeout);
        other.requestTimeout(requestTimeout);
        other.incomingCapacity(incomingCapacity);
        other.outgoingCapacity(outgoingCapacity);
        other.defaultNextReceiverPolicy(nextReceiverPolicy);

        if (offeredCapabilities != null) {
            other.offeredCapabilities(Arrays.copyOf(offeredCapabilities, offeredCapabilities.length));
        }
        if (desiredCapabilities != null) {
            other.desiredCapabilities(Arrays.copyOf(desiredCapabilities, desiredCapabilities.length));
        }
        if (properties != null) {
            other.properties(new HashMap<>(properties));
        }

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
     * a {@link Session} as been honored.
     *
     * @param closeTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions closeTimeout(long closeTimeout) {
        return closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to close
     * a resource such as a {@link Session}, {@link Sender} or {@link Receiver} h as been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions closeTimeout(long timeout, TimeUnit units) {
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
     * a {@link Session} has been honored.
     *
     * @param openTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions openTimeout(long openTimeout) {
        return openTimeout(openTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a response from the remote that a request to open
     * a resource such as a {@link Session}, {@link Sender} or {@link Receiver} has been honored.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions openTimeout(long timeout, TimeUnit units) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions sendTimeout(long sendTimeout) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions sendTimeout(long timeout, TimeUnit units) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions requestTimeout(long requestTimeout) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions requestTimeout(long timeout, TimeUnit units) {
        this.requestTimeout = units.toMillis(timeout);
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions drainTimeout(long drainTimeout) {
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
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions drainTimeout(long timeout, TimeUnit units) {
        this.drainTimeout = units.toMillis(timeout);
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
     * Sets the incoming capacity for a {@link Session} created with these options.  The incoming capacity
     * controls how much buffering a session will allow before applying back pressure to the remote
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

    /**
     * @return the outgoing capacity limit that is configured for newly created {@link Session} instances.
     */
    public int outgoingCapacity() {
        return outgoingCapacity;
    }

    /**
     * Sets the outgoing capacity for a {@link Session} created with these options.  The outgoing capacity
     * controls how much buffering a session will allow before applying back pressure to the local
     * thereby preventing excessive memory overhead while writing large amounts of data and the
     * client is experiencing back-pressure due to the remote not keeping pace.
     * <p>
     * This is an advanced option and in most cases the client defaults should be left in place unless
     * a specific issue needs to be addressed.  Setting this value incorrectly can lead to senders that
     * either block frequently or experience very poor overall performance.
     *
     * @param outgoingCapacity
     *      the outgoing capacity to set when creating a new {@link Session}.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions outgoingCapacity(int outgoingCapacity) {
        this.outgoingCapacity = outgoingCapacity;
        return this;
    }

    /**
     * @return the configured default next receiver policy for a session created using these options.
     */
    public NextReceiverPolicy defaultNextReceiverPolicy() {
        return nextReceiverPolicy;
    }

    /**
     * Configures the default next receiver policy for the session created with these
     * configuration options.
     *
     * @param policy
     * 		The default next receiver policy to assign to a new session.
     *
     * @return this {@link SessionOptions} instance.
     */
    public SessionOptions defaultNextReceiverPolicy(NextReceiverPolicy policy) {
        this.nextReceiverPolicy = policy;
        return this;
    }
}
