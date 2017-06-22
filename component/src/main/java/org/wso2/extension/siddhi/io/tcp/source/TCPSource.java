/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.tcp.source;

import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.util.Map;

/**
 * Tcp source extension.
 */
@Extension(
        name = "tcp",
        namespace = "source",
        description = "TBD",
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class TCPSource extends Source {
    private static final String CONTEXT = "context";
    private SourceEventListener sourceEventListener;
    private String context;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        context = optionHolder.validateAndGetStaticValue(CONTEXT,
                siddhiAppContext.getName() + "/" + sourceEventListener.getStreamDefinition().getId());
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        TCPServer.getInstance().addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return context;
            }

            @Override
            public void onMessage(byte[] message) {
                sourceEventListener.onEvent(message);
            }
        });
        TCPServer.getInstance().start();
    }

    @Override
    public void disconnect() {
        TCPServer.getInstance().removeStreamListener(context);
        TCPServer.getInstance().stop();
    }

    @Override
    public void destroy() {
    }

    @Override
    public void pause() {
        TCPServer.getInstance().pause();
    }

    @Override
    public void resume() {
        TCPServer.getInstance().resume();
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //no state
    }
}
