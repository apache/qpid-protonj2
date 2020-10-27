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
 * Container Options for customizing the behavior of the Container
 */
public class ClientOptions {

    private String id;
    private String futureType;

    public ClientOptions() {}

    public ClientOptions(ClientOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * @return the ID configured the Container
     */
    public String id() {
        return id;
    }

    /**
     * Sets the container ID that should be used when creating Connections
     *
     * @param id
     *      The container Id that should be assigned to container connections.
     *
     * @return this options class for chaining.
     */
    public ClientOptions id(String id) {
        this.id = id;
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
     * the futures used by the API.  By default the client will select a Future implementation
     * by itself however the user can override this selection here if desired.
     *
     * @param futureType
     *      The name of the future type to use.
     *
     * @return this options object for chaining.
     */
    public ClientOptions futureType(String futureType) {
        this.futureType = futureType;
        return this;
    }

    /**
     * Copy all options from this {@link ClientOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    public ClientOptions copyInto(ClientOptions other) {
        other.id(id);
        other.futureType(futureType);

        return this;
    }
}
