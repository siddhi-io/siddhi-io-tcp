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

package org.wso2.extension.siddhi.io.tcp.sink;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Tcp sink extension.
 */
@Extension(
        name = "tcp",
        namespace = "sink",
        description = "TBD",
        examples = @Example(description = "TBD", syntax = "TBD")
)
public class TCPSink extends Sink {

    public static final String URL = "url";
    public static final String SYNC = "sync";
    private static final Logger log = Logger.getLogger(TCPSink.class);
    private TCPNettyClient tcpNettyClient;
    private String host;
    private int port;
    private String channelId;
    private Option syncOption;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{SYNC};
    }

    @Override
    protected void init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                        ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        tcpNettyClient = new TCPNettyClient();
        String url = optionHolder.validateAndGetStaticValue(URL);
        syncOption = optionHolder.validateAndGetOption(SYNC);
        try {
            URL aURL = new URL(url);
            if (!aURL.getProtocol().equals("tcp")) {
                throw new SiddhiAppCreationException("Malformed url '" + url + "' with wrong protocol found, " +
                        "expected in format 'tcp://<host>:<port>/<context>'");
            }
            host = aURL.getHost();
            port = aURL.getPort();
            channelId = aURL.getPath();
        } catch (MalformedURLException e) {
            throw new SiddhiAppCreationException("Malformed url '" + url + "' found, expected in format " +
                    "'tcp://<host>:<port>/<context>'", e);
        }

    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        tcpNettyClient.connect(host, port);
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
        if (payload instanceof String) {
            tcpNettyClient.send(channelId, ((String) payload).getBytes(Charset.defaultCharset()));
        } else {
            tcpNettyClient.send(channelId, (byte[]) payload);
        }
    }

    @Override
    public void disconnect() {
        if (tcpNettyClient != null) {
            tcpNettyClient.disconnect();
        }
    }

    @Override
    public void destroy() {
        if (tcpNettyClient != null) {
            tcpNettyClient.shutdown();
            tcpNettyClient = null;
        }
    }


    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        // no state
    }
}
