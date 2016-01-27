/*
 *
 *  Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.wso2.das.javaagent.instrumentation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;
import org.wso2.das.javaagent.exception.InstrumentationAgentException;
import org.wso2.das.javaagent.schema.AgentConnection;
import org.wso2.das.javaagent.schema.InstrumentationAgent;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.util.Map;

/**
 * AgentPublisherHolder handles the publishing of events to DAS server.
 */
public class AgentPublisherHolder {

    private static final Log log = LogFactory.getLog(AgentPublisherHolder.class);
    private static AgentPublisherHolder instance = null;
    private static String streamId;
    private static DataPublisher dataPublisher;
    protected static final String THRIFT_AGENT_TYPE = "Thrift";

    protected AgentPublisherHolder() {
        try {
            makeAgentConnection();
        } catch (JAXBException e) {
            log.error("Error parsing agent configuration file.", e);
        } catch (InstrumentationAgentException e) {
            log.error(e);
        }
    }

    public static AgentPublisherHolder getInstance() {
        if (instance == null) {
            instance = new AgentPublisherHolder();
        }
        return instance;
    }

    private void makeAgentConnection() throws JAXBException, InstrumentationAgentException {
        InstrumentingAgent agent = new InstrumentingAgent();
        InstrumentationDataHolder instDataHolder = InstrumentationDataHolder.getInstance();
        agent.setConfigurationFilePath(InstrumentingAgent.args);
//        agent.setLoggingConfiguration();
        File file = new File(createAgentConfigFilepath(instDataHolder));
        JAXBContext jaxbContext = JAXBContext.newInstance(InstrumentationAgent.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        InstrumentationAgent instAgent = (InstrumentationAgent) jaxbUnmarshaller.unmarshal(file);
        AgentConnection agentConnection = instAgent.getAgentConnection();
        if (agentConnection != null) {
            addAgentConfiguration(agentConnection);
        }
    }

    private String createAgentConfigFilepath(InstrumentationDataHolder instDataHolder) {
        String filePath = instDataHolder.getConfigFilePathHolder();
        if (instDataHolder.isCarbonProduct()) {
            filePath += File.separator + "repository" + File.separator + "conf" + File.separator + "javaagent"
                    + File.separator;
        }
        filePath += "inst-agent-config.xml";
        return filePath;
    }

    public void addAgentConfiguration(AgentConnection agentConnection) throws InstrumentationAgentException {
        locateConfigurationFiles();
        setupAgentPublisher(agentConnection, agentConnection.getStreamName(), agentConnection.getStreamVersion());
        if (log.isDebugEnabled()) {
            log.debug("Publisher created successfully");
        }
    }

    private void locateConfigurationFiles() {
        InstrumentationDataHolder dataHolder = InstrumentationDataHolder.getInstance();
        String trustStorePath = dataHolder.getConfigFilePathHolder();
        if (dataHolder.isCarbonProduct()) {
            trustStorePath += File.separator + "repository" + File.separator + "resources" + File.separator
                    + "security" + File.separator;
        }
        trustStorePath += "client-truststore.jks";
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");
        if (!dataHolder.isCarbonProduct()) {
            AgentHolder.setConfigPath(dataHolder.getConfigFilePathHolder() + "data-agent-config.xml");
        }
    }

    private void setupAgentPublisher(AgentConnection agentConnection, String agentStream, String version)
            throws InstrumentationAgentException {
        try {
            dataPublisher = new DataPublisher(THRIFT_AGENT_TYPE, agentConnection.getReceiverURL(),
                    agentConnection.getAuthURL(), agentConnection.getUsername(), agentConnection.getPassword());
        } catch (DataEndpointException | DataEndpointAgentConfigurationException | DataEndpointAuthenticationException
                | TransportException e) {
            throw new InstrumentationAgentException("Failed to establish connection with server : " + e.getMessage(), e);
        } catch (DataEndpointConfigurationException e) {
            throw new InstrumentationAgentException("Failed to initialize agent publisher : " + e.getMessage(), e);
        }
        streamId = DataBridgeCommonsUtils.generateStreamId(agentStream, version);
    }


    /**
     * Publish the obtained queries to DAS using normal publish method which passes
     * only metadata, correlation data and payload data. Five parameters concatenated in
     * payload data (scenario name, class name, method name, instrumentation location, duration)
     * would be separated into an object array.
     *
     * @param timeStamp   current timestamp
     * @param payloadData string containing payload data values
     */
    public static void publishEvents(long timeStamp, long correlationData, String payloadData) {
        Object[] payload = payloadData.split(":");
        Object[] correlation = {correlationData};
        if(dataPublisher != null) {
            dataPublisher.publish(streamId, timeStamp, null, correlation, payload, null);
        }
    }

    /**
     * Overloaded the above publishEvents method, with extra parameter to pass
     * key,value pairs obtained in situations with extra attributes.
     *
     * @param timeStamp    current time in milli seconds
     * @param payloadData  string containing payload data values
     * @param arbitraryMap map containing <key,value> pairs of parameters
     */
    public static void publishEvents(long timeStamp, long correlationData, String payloadData, Map<String, String> arbitraryMap) {
        Object[] payload = payloadData.split(":");
        Object[] correlation = {correlationData};
        if(dataPublisher != null) {
            dataPublisher.publish(streamId, timeStamp, null, correlation, payload, arbitraryMap);
        }
    }
}
