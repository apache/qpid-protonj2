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
package org.apache.qpid.protonj2.client.transport.netty5;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.buffer.Buffer;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.SimpleChannelInboundHandler;

/**
 * Simple Netty Server used to echo all data.
 */
public class NettyEchoServer extends NettyServer {

    private static final Logger LOG = LoggerFactory.getLogger(NettyEchoServer.class);

    public NettyEchoServer(TransportOptions options, SslOptions sslOptions, boolean needClientAuth) {
        super(options, sslOptions, needClientAuth);
    }

    @Override
    protected ChannelHandler getServerHandler() {
        return new EchoServerHandler();
    }

    private class EchoServerHandler extends SimpleChannelInboundHandler<Buffer>  {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, Buffer msg) {
            LOG.trace("Channel read: {}", msg);
            ctx.write(msg.copy());
        }
    }
}
