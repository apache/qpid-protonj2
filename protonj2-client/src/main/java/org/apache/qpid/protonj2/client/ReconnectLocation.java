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

import java.util.Objects;

/**
 * Represents a reconnection host used to track location and configuration
 * for individual remote hosts that can be used to re-establish a connection
 * on loss of connectivity.
 */
public class ReconnectLocation {

	private final String host;
	private final int port;

	public ReconnectLocation(String host, int port) {
		Objects.requireNonNull(host, "Cannot create a reconnect entry with a null host value");

		if (host.isBlank()) {
			throw new IllegalArgumentException("Cannot create a reconnect entry with a blank host value");
		}

		this.host = host;
		this.port = port;
	}

	/**
	 * @return the host where the reconnect should attempt its reconnection.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return the port where the reconnect should attempt its connection.
	 */
	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		ReconnectLocation other = (ReconnectLocation) obj;
		if (host == null) {
			if (other.host != null) {
				return false;
			}
		} else if (!host.equalsIgnoreCase(other.host)) {
			return false;
		}

		if (port != other.port) {
			return false;
		}

		return true;
	}
}
