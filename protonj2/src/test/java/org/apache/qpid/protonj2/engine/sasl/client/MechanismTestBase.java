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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;

/**
 * Base class for SASL Mechanism tests that provides some default utilities
 */
public class MechanismTestBase {

    protected static final String HOST = "localhost";
    protected static final String USERNAME = "user";
    protected static final String PASSWORD = "pencil";

    protected static final ProtonBuffer TEST_BUFFER = ProtonByteBufferAllocator.DEFAULT.allocate(10, 10).setWriteIndex(10);

    protected SaslCredentialsProvider credentials() {
        return new UserCredentialsProvider(USERNAME, PASSWORD, HOST, true);
    }

    protected SaslCredentialsProvider credentials(String user, String password) {
        return new UserCredentialsProvider(user, password, null, false);
    }

    protected SaslCredentialsProvider credentials(String user, String password, boolean principal) {
        return new UserCredentialsProvider(user, password, null, principal);
    }

    protected SaslCredentialsProvider emptyCredentials() {
        return new UserCredentialsProvider(null, null, null, false);
    }

    private static class UserCredentialsProvider implements SaslCredentialsProvider {

        private final String username;
        private final String password;
        private final String host;
        private final boolean principal;

        public UserCredentialsProvider(String username, String password, String host, boolean principal) {
            this.username = username;
            this.password = password;
            this.host = host;
            this.principal = principal;
        }

        @Override
        public String vhost() {
            return host;
        }

        @Override
        public String username() {
            return username;
        }

        @Override
        public String password() {
            return password;
        }

        @Override
        public Principal localPrincipal() {
            if (principal) {
                return new Principal() {

                    @Override
                    public String getName() {
                        return "TEST-Principal";
                    }
                };
            } else {
                return null;
            }
        }
    }
}
