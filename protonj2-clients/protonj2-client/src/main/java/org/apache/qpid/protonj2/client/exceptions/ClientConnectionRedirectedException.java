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

import java.net.URI;

import org.apache.qpid.protonj2.client.ErrorCondition;

/**
 * A {@link ClientIOException} type that defines that the remote peer has requested that this
 * connection be redirected to some alternative peer.  The redirect information can be obtained
 * by calling the {@link ClientConnectionRedirectedException#getRedirectionURI()} method which
 * return the URI of the peer the client is being redirect to.
 */
public class ClientConnectionRedirectedException extends ClientConnectionRemotelyClosedException {

    private static final long serialVersionUID = 5872211116061710369L;

    private final URI redirect;

    public ClientConnectionRedirectedException(String reason, URI redirect, ErrorCondition condition) {
        super(reason, condition);

        this.redirect = redirect;
    }

    /**
     * @return the URI that represents the redirection.
     */
    public URI getRedirectionURI() {
        return redirect;
    }
}
