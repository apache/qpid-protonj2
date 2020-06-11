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
package org.apache.qpid.proton4j.logging;

import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;

/**
 * Slf4j adapter class used to proxy calls to the slf4j logger
 * factory and create ProtonLogger wrappers around the Loggers
 * for that library.
 */
public class Slf4JLoggerFactory extends ProtonLoggerFactory {

    /*
     * Must be created via the static find method.
     */
    private Slf4JLoggerFactory() {
    }

    public static ProtonLoggerFactory findLoggerFactory() {
        // We don't support the NO-OP logger and instead will fall back to our own variant
        if (LoggerFactory.getILoggerFactory() instanceof NOPLoggerFactory) {
            throw new NoClassDefFoundError("Slf4j NOPLoggerFactory is not supported by this library");
        }

        return new Slf4JLoggerFactory();
    }

    @Override
    protected ProtonLogger createLoggerWrapper(String name) {
        return new Slf4JLoggerWrapper(LoggerFactory.getLogger(name));
    }
}
