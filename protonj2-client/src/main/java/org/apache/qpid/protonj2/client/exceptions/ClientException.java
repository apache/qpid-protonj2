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
package org.apache.qpid.protonj2.client.exceptions;

import org.apache.qpid.protonj2.client.Connection;

/**
 * Represents a non-fatal exception that occurs from a Client {@link Connection}
 * or one of its resources.  These error types can typically be recovered from
 * without a full tear down and rebuild of the connection.  One example might be a
 * failure to commit a transaction due to a forced roll back on the remote side
 * of the connection.
 */
public class ClientException extends Exception {

    private static final long serialVersionUID = -5094579928657311571L;

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
