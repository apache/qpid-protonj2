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
package org.apache.qpid.protonj2.engine.sasl.client;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;

/**
 * Interface for a supplier of login credentials used by the SASL Authenticator to
 * select and configure the client SASL mechanism.
 */
public interface SaslCredentialsProvider {

    /**
     * @return the virtual host value to use when performing SASL authentication.
     */
    String vhost();

    /**
     * @return the user name value to use when performing SASL authentication.
     */
    String username();

    /**
     * @return the password value to use when performing SASL authentication.
     */
    String password();

    /**
     * @return the local principal value to use when performing SASL authentication.
     */
    Principal localPrincipal();

    /**
     * @return a {@link Map} of optional values to use when performing SASL authentication.
     */
    @SuppressWarnings("unchecked")
    default Map<String, Object> options() {
        return Collections.EMPTY_MAP;
    }
}
