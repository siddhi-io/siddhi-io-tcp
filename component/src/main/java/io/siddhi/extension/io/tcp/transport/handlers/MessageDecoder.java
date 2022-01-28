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

package io.siddhi.extension.io.tcp.transport.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.siddhi.extension.io.tcp.transport.callback.StreamListener;
import io.siddhi.extension.io.tcp.transport.utils.BinaryMessageConverterUtil;
import io.siddhi.extension.io.tcp.transport.utils.FlowController;
import io.siddhi.extension.io.tcp.transport.utils.StreamListenerHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Byte to message decoder.
 */
public class MessageDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = LogManager.getLogger(MessageDecoder.class);
    private StreamListenerHolder streamInfoHolder;
    private FlowController flowController;

    public MessageDecoder(StreamListenerHolder streamInfoHolder, FlowController flowController) {
        this.streamInfoHolder = streamInfoHolder;
        this.flowController = flowController;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 5) {
            return;
        }
        try {
            int protocol = in.readByte();
            int messageSize = in.readInt();
            if (protocol != 2 || messageSize > in.readableBytes()) {
                in.resetReaderIndex();
                return;
            }

            flowController.barrier();

            int sessionIdSize = in.readInt();
            BinaryMessageConverterUtil.getString(in, sessionIdSize);

            int channelIdSize = in.readInt();
            String channelId = BinaryMessageConverterUtil.getString(in, channelIdSize);

            int dataLength = in.readInt();
            byte[] bytes = new byte[dataLength];
            in.readBytes(bytes);

            StreamListener streamListener = streamInfoHolder.getStreamListener(channelId);
            if (streamListener == null) {
                in.markReaderIndex();
                LOG.error("Events with unknown channelId : '" + channelId + "' hence dropping the events!");
                return;
            }
            streamListener.onMessage(bytes);

        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            in.markReaderIndex();
        }

    }

}
