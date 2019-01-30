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
package org.apache.qpid.proton4j.engine;

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;

/**
 * Base API for {@link Sender} and {@link Receiver} links.
 */
public interface Link extends Endpoint {

    /**
     * @return the parent {@link Session} of the Receiver.
     */
    Session getSession();

    /**
     * @return the link name that is assigned to this link.
     */
    String getName();

    /**
     * @return the {@link Source} for the local end of this link.
     */
    Source getSource();

    /**
     * @return the {@link Target} for the local end of this link.
     */
    Target getTarget();

    /**
     * Gets the local link properties.
     *
     * @see #setProperties(Map)
     */
    Map<Symbol, Object> getProperties();

    /**
     * Sets the local link offered capabilities, to be conveyed to the peer via the Attach frame
     * when attaching the link to the session.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param offeredCapabilities
     *          the offered capabilities array to send, or null for none.
     */
    public void setOfferedCapabilities(Symbol[] offeredCapabilities);

    /**
     * Gets the local link offered capabilities.
     *
     * @return the offered capabilities array, or null if none was set.
     *
     * @see #setOfferedCapabilities(Symbol[])
     */
    Symbol[] getOfferedCapabilities();

    /**
     * Sets the local link desired capabilities, to be conveyed to the peer via the Attach frame
     * when attaching the link to the session.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param desiredCapabilities
     *          the desired capabilities array to send, or null for none.
     */
    public void setDesiredCapabilities(Symbol[] desiredCapabilities);

    /**
     * Gets the local link desired capabilities.
     *
     * @return the desired capabilities array, or null if none was set.
     *
     * @see #setDesiredCapabilities(Symbol[])
     */
    Symbol[] getDesiredCapabilities();

    /**
     * Sets the local link properties, to be conveyed to the peer via the Attach frame when
     * attaching the link to the session.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     */
    void setProperties(Map<Symbol, Object> properties);

    /**
     * Sets the local link max message size, to be conveyed to the peer via the Attach frame
     * when attaching the link to the session. Null or 0 means no limit.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param maxMessageSize
     *            the local max message size value, or null to clear. 0 also means no limit.
     */
    void setMaxMessageSize(UnsignedLong maxMessageSize);

    /**
     * Gets the local link max message size.
     *
     * @return the local max message size, or null if none was set. 0 also means no limit.
     *
     * @see #setMaxMessageSize(UnsignedLong)
     */
    UnsignedLong getMaxMessageSize();

    //----- View of the state of the link at the remote

    /**
     * @return the {@link Source} for the remote end of this link.
     */
    Source getRemoteSource();

    /**
     * @return the {@link Target} for the remote end of this link.
     */
    Target getRemoteTarget();

    /**
     * Gets the remote link offered capabilities, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the offered capabilities array conveyed by the peer, or null if there was none.
     */
    Symbol[] getRemoteOfferedCapabilities();

    /**
     * Gets the remote link desired capabilities, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the desired capabilities array conveyed by the peer, or null if there was none.
     */
    Symbol[] getRemoteDesiredCapabilities();

    /**
     * Gets the remote link properties, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the properties Map conveyed by the peer, or null if there was none.
     */
    Map<Symbol, Object> getRemoteProperties();

    /**
     * Gets the remote link max message size, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the remote max message size conveyed by the peer, or null if none was set. 0 also means no limit.
     */
    UnsignedLong getRemoteMaxMessageSize();

}
