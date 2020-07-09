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
package org.apache.qpid.protonj2.client.impl;

import static org.apache.qpid.protonj2.client.impl.ClientConstants.CONTAINER_ID;
import static org.apache.qpid.protonj2.client.impl.ClientConstants.INVALID_FIELD;

import java.util.Map;

import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRedirectedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionResourceAllocationException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionResourceNotFoundException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecurityException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientInvalidContainerIDException;
import org.apache.qpid.protonj2.client.exceptions.ClientInvalidDestinationException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceAllocationException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceDeletedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSecurityException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionRolledBackException;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transactions.TransactionErrors;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Support methods for working with AMQP ErrorCondition types
 */
public abstract class ClientErrorSupport {

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates the connection creation failed for some reason.
     *
     * @param connection
     * 		the AMQP Client instance that originates this exception
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ClientConnectionRemotelyClosedException convertToConnectionClosedException(ClientConnection connection, ErrorCondition errorCondition) {
        ClientConnectionRemotelyClosedException remoteError = null;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                remoteError = new ClientConnectionSecurityException(message);
            } else if (error.equals(AmqpError.RESOURCE_LIMIT_EXCEEDED)) {
                remoteError = new ClientConnectionResourceAllocationException(message);
            } else if (error.equals(ConnectionError.CONNECTION_FORCED)) {
                remoteError = new ClientConnectionRemotelyClosedException(message);
            } else if (error.equals(AmqpError.NOT_FOUND)) {
                remoteError = new ClientConnectionResourceNotFoundException(message);
            } else if (error.equals(ConnectionError.REDIRECT)) {
                remoteError = createRedirectException(connection, error, message, errorCondition);
            } else if (error.equals(AmqpError.INVALID_FIELD)) {
                Map<?, ?> info = errorCondition.getInfo();
                if (info != null && CONTAINER_ID.equals(info.get(INVALID_FIELD))) {
                    remoteError = new ClientInvalidContainerIDException(message);
                } else {
                    remoteError = new ClientConnectionRemotelyClosedException(message);
                }
            } else {
                remoteError = new ClientConnectionRemotelyClosedException(message);
            }
        } else if (remoteError == null) {
            remoteError = new ClientConnectionRemotelyClosedException("Unknown error from remote peer");
        }

        return remoteError;
    }

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates a non-fatal error usually at the link level
     * such as link closed remotely or link create failed due to security access
     * issues.
     *
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ClientException convertToNonFatalException(ErrorCondition errorCondition) {
        ClientException remoteError = null;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                remoteError = new ClientSecurityException(message);
            } else if (error.equals(AmqpError.RESOURCE_LIMIT_EXCEEDED)) {
                remoteError = new ClientResourceAllocationException(message);
            } else if (error.equals(AmqpError.NOT_FOUND)) {
                remoteError = new ClientInvalidDestinationException(message);
            } else if (error.equals(AmqpError.RESOURCE_DELETED)) {
                remoteError = new ClientResourceDeletedException(message);
            } else if (error.equals(TransactionErrors.TRANSACTION_ROLLBACK)) {
                remoteError = new ClientTransactionRolledBackException(message);
            } else {
                remoteError = new ClientException(message);
            }
        } else if (remoteError == null) {
            remoteError = new ClientException("Unknown error from remote peer");
        }

        return remoteError;
    }

    /**
     * Attempt to read and return the embedded error message in the given ErrorCondition
     * object.  If no message can be extracted a generic message is returned.
     *
     * @param errorCondition
     *      The ErrorCondition to extract the error message from.
     *
     * @return an error message extracted from the given ErrorCondition.
     */
    public static String extractErrorMessage(ErrorCondition errorCondition) {
        String message = "Received error from remote peer without description";
        if (errorCondition != null) {
            if (errorCondition.getDescription() != null && !errorCondition.getDescription().isEmpty()) {
                message = errorCondition.getDescription();
            }

            Symbol condition = errorCondition.getCondition();
            if (condition != null) {
                message = message + " [condition = " + condition + "]";
            }
        }

        return message;
    }

    /**
     * When a redirect type exception is received this method is called to create the
     * appropriate redirect exception type containing the error details needed.
     *
     * @param connection
     * 		  the AMQP Client instance that originates this exception
     * @param error
     *        the Symbol that defines the redirection error type.
     * @param message
     *        the basic error message that should used or amended for the returned exception.
     * @param condition
     *        the ErrorCondition that describes the redirection.
     *
     * @return an Exception that captures the details of the redirection error.
     */
    public static ClientConnectionRemotelyClosedException createRedirectException(ClientConnection connection, Symbol error, String message, ErrorCondition condition) {
        ClientConnectionRemotelyClosedException result = null;
        Map<?, ?> info = condition.getInfo();

        if (info == null) {
            result = new ClientConnectionRemotelyClosedException(message + " : Redirection information not set.");
        } else {
            @SuppressWarnings("unchecked")
            ClientRedirect redirect = new ClientRedirect((Map<Symbol, Object>) info, connection);

            try {
                result = new ClientConnectionRedirectedException(message, redirect.validate().toURI());
            } catch (Exception ex) {
                result = new ClientConnectionRemotelyClosedException(message + " : " + ex.getMessage());
            }
        }

        return result;
    }
}
