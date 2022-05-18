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

/**
 * Determines the behavior of a Session when the next receiver method is called
 * on that session. Each policy provides a contract on the ordering of returned
 * receivers from the next receiver API when there are receivers with locally
 * queued deliveries. When there are no {@link Receiver} instances that have
 * locally queued deliveries the next receive API will return the next receiver
 * to receive a complete incoming delivery unless a timeout was given and that
 * time period expires in which case it will return <code>null</code>.
 * <p>
 * Should the user perform receive calls on a {@link Receiver} directly in multiple
 * threads the behavior of the next receiver API is undefined and it becomes possible
 * that the resulting receiver returned from that API will have no actual pending
 * deliveries due to a race. In most cases the caller can mitigate some risk by using
 * the {@link Receiver#tryReceive()} API and accounting for a null result.
 */
public enum NextReceiverPolicy {

    /**
     * Examines the list of currently open receivers in the session and returns
     * the next receiver that has a pending delivery that follows the previously
     * returned receiver (if any) otherwise the first receiver in the session with
     * a pending delivery is returned. The order of receivers returned will likely
     * be creation order however the implementation is not required to follow this
     * pattern so the caller should not be coded to rely on that ordering.
     */
    ROUND_ROBIN,

    /**
     * Examines the list of currently open receivers in the session and returns a
     * random selection from the set of receivers that have a pending delivery
     * immediately available. This provides a means of selecting receivers which
     * is not prone to sticking to a highly active receiver which can starve out
     * other receivers which receive only limited traffic.
     */
    RANDOM,

    /**
     * Examines the list of currently open receivers in the session and returns the
     * first receiver found with an available delivery. This can result in starvation
     * if that receiver has a continuous feed of new deliveries from the remote as it
     * will be repeatedly selected by the next receiver API.
     */
    FIRST_AVAILABLE,

    /**
     * Examines the list of currently open receivers in the session and returns the
     * receiver with the largest backlog of available deliveries. This can result in
     * starvation if that receiver has a continuous feed of new deliveries from the
     * remote as it will likely be repeatedly selected by the next receiver API.
     */
    LARGEST_BACKLOG,

    /**
     * Examines the list of currently open receivers in the session and returns the
     * receiver with the smallest backlog of available deliveries.
     */
    SMALLEST_BACKLOG

}
