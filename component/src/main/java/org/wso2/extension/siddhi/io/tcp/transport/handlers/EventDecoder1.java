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

package org.wso2.extension.siddhi.io.tcp.transport.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.converter.SiddhiEventConverter;
import org.wso2.extension.siddhi.io.tcp.transport.utils.BinaryMessageConverterUtil;
import org.wso2.extension.siddhi.io.tcp.transport.utils.StreamInfo;
import org.wso2.extension.siddhi.io.tcp.transport.utils.StreamTypeHolder;

import java.io.UnsupportedEncodingException;
import java.util.List;


/**
 * Byte to Siddhi event decoder.
 */
public class EventDecoder1 extends ByteToMessageDecoder {
    static final Logger LOG = Logger.getLogger(SiddhiEventConverter.class);
    private StreamTypeHolder streamInfoHolder;

    public EventDecoder1(StreamTypeHolder streamInfoHolder) {
        this.streamInfoHolder = streamInfoHolder;
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

            int sessionIdSize = in.readInt();
            BinaryMessageConverterUtil.getString(in, sessionIdSize);

            int streamIdSize = in.readInt();
            String streamId = BinaryMessageConverterUtil.getString(in, streamIdSize);

            int dataLength = in.readInt();
            byte[] bytes = new byte[dataLength];
            in.readBytes(bytes);

            StreamInfo streamInfo = streamInfoHolder.getStreamInfo(streamId);

            if (streamInfo == null) {
                in.markReaderIndex();
                LOG.error("Events with unknown streamId : '" + streamId + "' hence dropping the events!");
                return;
            }

            streamInfo.getStreamListener().onEvent(bytes);

        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            in.markReaderIndex();
        }

    }
}
