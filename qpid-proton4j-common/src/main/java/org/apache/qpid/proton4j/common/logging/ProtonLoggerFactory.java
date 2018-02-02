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
 * Factory used to create Proton Logger abstractions
 */
public abstract class ProtonLoggerFactory {

    private static volatile ProtonLoggerFactory loggerFactory;

    /**
     * Returns a {@link ProtonLoggerFactory} instance.
     * <p>
     * The factory returned depends on the configuration and available
     * logger implementations at the time this method is called.  If a
     * custom ProtonLoggerFactory is configured than that instance is
     * returned, otherwise this class will attempt to find a factory for
     * the slf4j logger library.
     *
     * @return a {@link ProtonLoggerFactory} that will be used by this library.
     */
    public static ProtonLoggerFactory getLoggerFactory() {
        if (loggerFactory == null) {
            loggerFactory = findSupportedLoggingFramework();
        }

        return loggerFactory;
    }

    /**
     * Configure Proton with a custom ProtonLoggerFactory implementation which will
     * be used by the Proton classes when logging library events.
     *
     * @param factory
     *      The {@link ProtonLoggerFactory} to use when loggers are requested.
     *
     * @throws IllegalArgumentException if the factory given is null.
     */
    public static void setLoggerFactory(ProtonLoggerFactory factory) {
        if (loggerFactory == null) {
            throw new IllegalArgumentException("Cannot configure the logger factory as null");
        }

        ProtonLoggerFactory.loggerFactory = factory;
    }

    public static ProtonLogger getLogger(Class<?> clazz) {
        return getLoggerFactory().createLoggerWrapper(clazz.getName());
    }

    public static ProtonLogger getLogger(String name) {
        return getLoggerFactory().createLoggerWrapper(name);
    }

    protected abstract ProtonLogger createLoggerWrapper(String name);

    //----- Logger search ----------------------------------------------------//

    private static ProtonLoggerFactory findSupportedLoggingFramework() {
        ProtonLoggerFactory factory = null;

        try {
            factory = Slf4JLoggerFactory.findLoggerFactory();
            factory.createLoggerWrapper(ProtonLoggerFactory.class.getName()).debug(
                "SLF4J found and will be used as the logging framework");
        } catch (Throwable t1) {
            factory = NoOpProtonLoggerFactory.INSTANCE;
        }

        return factory;
    }
}
