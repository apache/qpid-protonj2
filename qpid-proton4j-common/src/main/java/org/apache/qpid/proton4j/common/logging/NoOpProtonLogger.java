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
package org.apache.qpid.proton4j.common.logging;

/**
 * Simple proton logger implementation that performs no logging.
 */
public class NoOpProtonLogger implements ProtonLogger {

    public static final NoOpProtonLogger INSTANCE = new NoOpProtonLogger();

    private NoOpProtonLogger() {
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public void trace(String message) {
    }

    @Override
    public void trace(String message, Object value) {
    }

    @Override
    public void trace(String message, Object value1, Object value2) {
    }

    @Override
    public void trace(String message, Object... arguments) {
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void debug(String message) {
    }

    @Override
    public void debug(String message, Object value) {
    }

    @Override
    public void debug(String message, Object value1, Object value2) {
    }

    @Override
    public void debug(String message, Object... arguments) {
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public void info(String message) {
    }

    @Override
    public void info(String message, Object value) {
    }

    @Override
    public void info(String message, Object value1, Object value2) {
    }

    @Override
    public void info(String message, Object... arguments) {
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void warn(String message) {
    }

    @Override
    public void warn(String message, Object value) {
    }

    @Override
    public void warn(String message, Object value1, Object value2) {
    }

    @Override
    public void warn(String message, Object... arguments) {
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public void error(String message) {
    }

    @Override
    public void erro(String message, Object value) {
    }

    @Override
    public void error(String message, Object value1, Object value2) {
    }

    @Override
    public void error(String message, Object... arguments) {
    }
}
