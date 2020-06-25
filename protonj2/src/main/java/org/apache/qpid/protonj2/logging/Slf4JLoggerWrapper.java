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
package org.apache.qpid.protonj2.logging;

import org.slf4j.Logger;

/**
 * A Wrapper around an Slf4J Logger used to proxy logging calls to that
 * framework when it is available.
 */
public class Slf4JLoggerWrapper implements ProtonLogger {

    private final transient Logger logger;

    Slf4JLoggerWrapper(Logger logger) {
        this.logger = logger;
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void trace(String message) {
        logger.trace(message);
    }

    @Override
    public void trace(String message, Object value) {
        logger.trace(message, value);
    }

    @Override
    public void trace(String message, Object value1, Object value2) {
        logger.trace(message, value1, value2);
    }

    @Override
    public void trace(String message, Object... arguments) {
        logger.trace(message, arguments);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String message) {
        logger.debug(message);
    }

    @Override
    public void debug(String message, Object value) {
        logger.debug(message, value);
    }

    @Override
    public void debug(String message, Object value1, Object value2) {
        logger.debug(message, value1, value2);
    }

    @Override
    public void debug(String message, Object... arguments) {
        logger.debug(message, arguments);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String message) {
        logger.info(message);
    }

    @Override
    public void info(String message, Object value) {
        logger.info(message, value);
    }

    @Override
    public void info(String message, Object value1, Object value2) {
        logger.info(message, value1, value2);
    }

    @Override
    public void info(String message, Object... arguments) {
        logger.info(message, arguments);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String message) {
        logger.warn(message);
    }

    @Override
    public void warn(String message, Object value) {
        logger.warn(message, value);
    }

    @Override
    public void warn(String message, Object value1, Object value2) {
        logger.warn(message, value1, value2);
    }

    @Override
    public void warn(String message, Object... arguments) {
        logger.warn(message, arguments);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String message) {
        logger.error(message);
    }

    @Override
    public void erro(String message, Object value) {
        logger.error(message, value);
    }

    @Override
    public void error(String message, Object value1, Object value2) {
        logger.error(message, value1, value2);
    }

    @Override
    public void error(String message, Object... arguments) {
        logger.error(message, arguments);
    }
}
