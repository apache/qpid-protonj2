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

import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRedirectedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecurityException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecuritySaslException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRedirectedException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSessionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientTransactionRolledBackException;
import org.apache.qpid.protonj2.engine.sasl.SaslSystemException;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transactions.TransactionErrors;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.LinkError;

public class ClientExceptionSupport {

    /**
     * Checks the given cause to determine if it's already an ProviderIOException type and
     * if not creates a new ProviderIOException to wrap it.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProviderIOException instance.
     */
    public static ClientIOException createOrPassthroughFatal(Throwable cause) {
        if (cause instanceof ClientIOException) {
            return (ClientIOException) cause;
        }

        if (cause.getCause() instanceof ClientIOException) {
            return (ClientIOException) cause.getCause();
        }

        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        return new ClientIOException(message, cause);
    }

    /**
     * Checks the given cause to determine if it's already an ProviderException type and
     * if not creates a new ProviderException to wrap it.  If the inbound exception is a
     * fatal type then it will pass through this method untouched to preserve the fatal
     * status of the error.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProviderException instance.
     */
    public static ClientException createNonFatalOrPassthrough(Throwable cause) {
        if (cause instanceof ClientException) {
            return (ClientException) cause;
        }

        if (cause.getCause() instanceof ClientException) {
            return (ClientException) cause.getCause();
        }

        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        if (cause instanceof TimeoutException) {
            return new ClientOperationTimedOutException(message, cause);
        } else {
            return new ClientException(message, cause);
        }
    }

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates the connection creation failed for some reason.
     *
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ClientConnectionRemotelyClosedException convertToConnectionClosedException(ErrorCondition errorCondition) {
        final ClientConnectionRemotelyClosedException remoteError;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                remoteError = new ClientConnectionSecurityException(message, new ClientErrorCondition(errorCondition));
            } else if (error.equals(ConnectionError.REDIRECT)) {
                remoteError = createConnectionRedirectException(error, message, errorCondition);
            } else {
                remoteError = new ClientConnectionRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
            }
        } else {
            remoteError = new ClientConnectionRemotelyClosedException("Unknown error from remote peer");
        }

        return remoteError;
    }

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates the connection creation failed for some reason.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ClientConnectionRemotelyClosedException convertToConnectionClosedException(Throwable cause) {
        ClientConnectionRemotelyClosedException remoteError = null;

        if (cause instanceof ClientConnectionRemotelyClosedException) {
            remoteError = (ClientConnectionRemotelyClosedException) cause;
        } else if (cause instanceof SaslSystemException) {
            remoteError = new ClientConnectionSecuritySaslException(
                cause.getMessage(), !((SaslSystemException) cause).isPermanent(), cause);
        } else if (cause instanceof SaslException) {
            remoteError = new ClientConnectionSecuritySaslException(cause.getMessage(), cause);
        } else {
            remoteError = new ClientConnectionRemotelyClosedException(cause.getMessage(), cause);
        }

        return remoteError;
    }

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates the connection creation failed for some reason.
     *
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ClientSessionRemotelyClosedException convertToSessionClosedException(ErrorCondition errorCondition) {
        final ClientSessionRemotelyClosedException remoteError;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            String message = extractErrorMessage(errorCondition);
            if (message == null) {
                message = "Session remotely closed without explanation";
            }

            remoteError = new ClientSessionRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
        } else {
            remoteError = new ClientSessionRemotelyClosedException("Session remotely closed without explanation");
        }

        return remoteError;
    }

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates the connection creation failed for some reason.
     *
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     * @param defaultMessage
     *      The message to use if the remote provided no condition for the closure
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ClientLinkRemotelyClosedException convertToLinkClosedException(ErrorCondition errorCondition, String defaultMessage) {
        final ClientLinkRemotelyClosedException remoteError;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            String message = extractErrorMessage(errorCondition);
            Symbol error = errorCondition.getCondition();

            if (message == null) {
                message = defaultMessage;
            }

            if (error.equals(LinkError.REDIRECT)) {
                remoteError = createLinkRedirectException(error, message, errorCondition);
            } else {
                remoteError = new ClientLinkRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
            }
        } else {
            remoteError = new ClientLinkRemotelyClosedException(defaultMessage);
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
        final ClientException remoteError;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.RESOURCE_LIMIT_EXCEEDED)) {
                remoteError = new ClientResourceRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
            } else if (error.equals(AmqpError.NOT_FOUND)) {
                remoteError = new ClientResourceRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
            } else if (error.equals(LinkError.DETACH_FORCED)) {
                remoteError = new ClientResourceRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
            } else if (error.equals(LinkError.REDIRECT)) {
                remoteError = createLinkRedirectException(error, message, errorCondition);
            } else if (error.equals(AmqpError.RESOURCE_DELETED)) {
                remoteError = new ClientResourceRemotelyClosedException(message, new ClientErrorCondition(errorCondition));
            } else if (error.equals(TransactionErrors.TRANSACTION_ROLLBACK)) {
                remoteError = new ClientTransactionRolledBackException(message);
            } else {
                remoteError = new ClientException(message);
            }
        } else {
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
     * When a connection redirect type exception is received this method is called to create the
     * appropriate redirect exception type containing the error details needed.
     *
     * @param error
     *        the Symbol that defines the redirection error type.
     * @param message
     *        the basic error message that should used or amended for the returned exception.
     * @param condition
     *        the ErrorCondition that describes the redirection.
     *
     * @return an Exception that captures the details of the redirection error.
     */
    public static ClientConnectionRemotelyClosedException createConnectionRedirectException(Symbol error, String message, ErrorCondition condition) {
        ClientConnectionRemotelyClosedException result;
        Map<?, ?> info = condition.getInfo();

        if (info == null) {
            result = new ClientConnectionRemotelyClosedException(
                message + " : Redirection information not set.", new ClientErrorCondition(condition));
        } else {
            @SuppressWarnings("unchecked")
            ClientRedirect redirect = new ClientRedirect((Map<Symbol, Object>) info);

            try {
                result = new ClientConnectionRedirectedException(
                    message, redirect.validate(), new ClientErrorCondition(condition));
            } catch (Exception ex) {
                result = new ClientConnectionRemotelyClosedException(
                    message + " : " + ex.getMessage(), new ClientErrorCondition(condition));
            }
        }

        return result;
    }

    /**
     * When a link redirect type exception is received this method is called to create the
     * appropriate redirect exception type containing the error details needed.
     *
     * @param error
     *        the Symbol that defines the redirection error type.
     * @param message
     *        the basic error message that should used or amended for the returned exception.
     * @param condition
     *        the ErrorCondition that describes the redirection.
     *
     * @return an Exception that captures the details of the redirection error.
     */
    public static ClientLinkRemotelyClosedException createLinkRedirectException(Symbol error, String message, ErrorCondition condition) {
        ClientLinkRemotelyClosedException result;
        Map<?, ?> info = condition.getInfo();

        if (info == null) {
            result = new ClientLinkRemotelyClosedException(
                message + " : Redirection information not set.", new ClientErrorCondition(condition));
        } else {
            @SuppressWarnings("unchecked")
            ClientRedirect redirect = new ClientRedirect((Map<Symbol, Object>) info);

            try {
                result = new ClientLinkRedirectedException(
                    message, redirect.validate(), new ClientErrorCondition(condition));
            } catch (Exception ex) {
                result = new ClientLinkRemotelyClosedException(
                    message + " : " + ex.getMessage(), new ClientErrorCondition(condition));
            }
        }

        return result;
    }
}
