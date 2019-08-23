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
package org.messaginghub.amqperative.client;

import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.util.IdGenerator;

/**
 * Container options used for default
 */
public final class ClientInstanceOptions extends ClientOptions {

    private final IdGenerator CONTAINER_ID_GENERATOR = new IdGenerator();

    private final String clientUniqueId = CONTAINER_ID_GENERATOR.generateId();

    public ClientInstanceOptions() {
        super();
        setContainerId(clientUniqueId);
    }

    public ClientInstanceOptions(ClientOptions options) {
        if (options != null) {
            options.copyInto(this);
        }

        if (getContainerId() == null || getContainerId().isEmpty()) {
            setContainerId(clientUniqueId);
        }
    }

    String getClientUniqueId() {
        return clientUniqueId;
    }
}
