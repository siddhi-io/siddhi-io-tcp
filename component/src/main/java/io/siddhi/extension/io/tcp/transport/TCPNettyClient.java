/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package io.siddhi.extension.io.tcp.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.extension.io.tcp.transport.handlers.MessageEncoder;
import io.siddhi.extension.io.tcp.transport.utils.EventComposite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

/**
 * Tcp Netty Client.
 */
public class TCPNettyClient {
    private static final Logger log = LogManager.getLogger(TCPNettyClient.class);

    private EventLoopGroup group;
    private Bootstrap bootstrap;
    private Channel channel;
    private String sessionId;
    private String hostAndPort;

    public TCPNettyClient(boolean keepAlive, boolean noDelay) {
        this(0, keepAlive, noDelay);
    }

    public TCPNettyClient() {
        this(0, true, true);
    }

    public TCPNettyClient(int numberOfThreads, boolean keepAlive, boolean noDelay) {
        group = new NioEventLoopGroup(numberOfThreads);
        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, noDelay)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addFirst(
                                new MessageEncoder()
                        );
                    }
                });
    }

    public void connect(String host, int port) throws ConnectionUnavailableException {
        // Start the connection attempt.
        try {
            hostAndPort = host + ":" + port;
            channel = bootstrap.connect(host, port).sync().channel();
            sessionId = UUID.randomUUID() + "-" + hostAndPort;
        } catch (Throwable e) {
            throw new ConnectionUnavailableException("Error connecting to '" + hostAndPort + "', " + e.getMessage(), e);
        }
    }

    public ChannelFuture send(final String channelId, final byte[] message) {
        EventComposite eventComposite = new EventComposite(sessionId, channelId, message);
        ChannelFuture cf = channel.writeAndFlush(eventComposite);
        return cf;
    }

    public void disconnect() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
                channel.closeFuture().sync();
            } catch (InterruptedException e) {
                log.error("Error closing connection to '" + hostAndPort + "' from client '" + sessionId +
                        "', " + e);
            }
            channel.disconnect();
            log.info("Disconnecting client to '" + hostAndPort + "' with sessionId:" + sessionId);
        }
    }

    public void shutdown() {
        disconnect();
        if (group != null) {
            group.shutdownGracefully();
        }
        log.info("Stopping client to '" + hostAndPort + "' with sessionId:" + sessionId);
        hostAndPort = null;
        sessionId = null;
    }

}



