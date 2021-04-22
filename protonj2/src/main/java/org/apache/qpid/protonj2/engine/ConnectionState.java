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
package org.apache.qpid.protonj2.engine;

/**
 * Represents the state of an AMQP Connection.  Each {@link Connection} provides both
 * a local and remote connection state that independently reflects the state at each
 * end.
 */
public enum ConnectionState {

	/**
	 * Indicates that the targeted end of the Connection (local or remote) has not yet been opened.
	 */
    IDLE,

	/**
	 * Indicates that the targeted end of the Connection (local or remote) is currently open.
	 */
    ACTIVE,

	/**
	 * Indicates that the targeted end of the Connection (local or remote) has been closed.
	 */
    CLOSED

}
