/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PostConstruct;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.ericsson.bss.cassandra.ecchronos.application.config.Interval;

/**
 * Service responsible for managing and scheduling checks on the list of nodes to find changes in the cluster
 * <p>
 * This service periodically checks the status of nodes removes or adds nodes depending on the changes.
 * </p>
 */

@Service
public final class ReloadNodesService implements DisposableBean
{

    private static final Logger LOG = LoggerFactory.getLogger(ReloadNodesService.class);
    private static final int DEFAULT_SCHEDULER_AWAIT_TERMINATION_IN_SECONDS = 60;
    private final EccNodesSync myEccNodesSync;
    private final DistributedJmxConnectionProvider myJmxConnectionProvider;
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;
    private final ScheduledExecutorService myScheduler = Executors.newScheduledThreadPool(1);
    private final Interval reLoadInterval;
    private final NodeListComparator nodeListComparator = new NodeListComparator();

    public ReloadNodesService(final EccNodesSync eccNodesSync,
                                 final Config config,
                                 final DistributedJmxConnectionProvider jmxConnectionProvider,
                                 final DistributedNativeConnectionProvider distributedNativeConnectionProvider
    )
    {
        this.myEccNodesSync = eccNodesSync;
        this.myJmxConnectionProvider = jmxConnectionProvider;
        this.myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
        this.reLoadInterval = config.getConnectionConfig().getReloadPolicy();
    }

    @PostConstruct
    public void startScheduler()
    {
        long reLoadIntervalInMills = reLoadInterval.getInterval(TimeUnit.MILLISECONDS);
        LOG.info("Starting ReloadNodesService with reLoadInterval={} ms", reLoadIntervalInMills);
        myScheduler.scheduleWithFixedDelay(this::reloadNodes, reLoadIntervalInMills,reLoadIntervalInMills,  TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    void reloadNodes()  {
        List<Node> oldNodes = myDistributedNativeConnectionProvider.getNodes();
        List<Node> newNodes = myDistributedNativeConnectionProvider.reloadNodes();
        CqlSession cqlSession = myDistributedNativeConnectionProvider.getCqlSession();
        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        if (!nodeChangeList.isEmpty()){
            myDistributedNativeConnectionProvider.setNodes(newNodes);
            Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();
            while (iterator.hasNext()){
                NodeChangeRecord nodeChangeRecord = iterator.next();
                if ( nodeChangeRecord.getType() == NodeChangeRecord.NodeChangeType.INSERT){
                    myEccNodesSync.verifyAcquireNode(nodeChangeRecord.getNode());
                    try {
                        myJmxConnectionProvider.add(nodeChangeRecord.getNode());
                    } catch (IOException e) {
                        LOG.info("Node {} JMX connection failed", nodeChangeRecord.getNode().getHostId() );
                    }

                }
                if ( nodeChangeRecord.getType() == NodeChangeRecord.NodeChangeType.DELETE){
                    myEccNodesSync.deleteNodeStatus(nodeChangeRecord.getNode().getDatacenter(),nodeChangeRecord.getNode().getHostId());
                    try {
                        myJmxConnectionProvider.close(nodeChangeRecord.getNode().getHostId());
                    }
                    catch (IOException e )
                    {
                        LOG.info("Node {} JMX connection removal failed", nodeChangeRecord.getNode().getHostId() );
                    }
                }
            }
        }


        
        
        /* 
        List<Node> unavailableNodes = findUnavailableNodes();

        if (unavailableNodes.isEmpty())
        {
            return;
        }

        unavailableNodes.forEach(this::retryConnectionForNode);
        */
    }

/*
    private List<Node> findUnavailableNodes()
    {
        List<Node> unavailableNodes = new ArrayList<>();
        ResultSet resultSet = myEccNodesSync.getResultSet();

        for (Row row : resultSet)
        {
            UUID nodeId = row.getUuid(COLUMN_NODE_ID);
            String status = Objects.requireNonNull(row.getString(COLUMN_NODE_STATUS)).toUpperCase(Locale.ENGLISH);

            if (NodeStatus.UNAVAILABLE.name().equals(status))
            {
                myDistributedNativeConnectionProvider.getNodes()
                        .stream()
                        .filter(node -> Objects.equals(node.getHostId(), nodeId))
                        .findFirst()
                        .ifPresent(unavailableNodes::add);
            }
        }

        return unavailableNodes;
    }

    private void retryConnectionForNode(final Node node)
    {
        UUID nodeId = node.getHostId();
        for (int attempt = 1; attempt <= retryBackoffStrategy.getMaxAttempts(); attempt++)
        {
            if (tryReconnectToNode(node, nodeId, attempt))
            {
                return; // Successfully reconnected, exit method
            }
        }
        markNodeUnreachable(node, nodeId);
    }

    private boolean tryReconnectToNode(final Node node, final UUID nodeId, final int attempt)
    {
        long delayMillis = retryBackoffStrategy.calculateDelay(attempt);
        LOG.warn("Attempting to reconnect to node: {}, attempt: {}", nodeId, attempt);

        if (establishConnectionToNode(node))
        {
            LOG.info("Successfully reconnected to node: {}", nodeId);
            myEccNodesSync.updateNodeStatus(NodeStatus.AVAILABLE, node.getDatacenter(), nodeId);
            return true;
        }
        else
        {
            LOG.warn("Failed to reconnect to node: {}, next retry in {} ms", nodeId, delayMillis);
            retryBackoffStrategy.sleepBeforeNextRetry(delayMillis);
            return false;
        }
    }

    private void markNodeUnreachable(final Node node, final UUID nodeId)
    {
        LOG.error("Max retry attempts reached for node: {}. Marking as UNREACHABLE.", nodeId);
        myEccNodesSync.updateNodeStatus(NodeStatus.UNREACHABLE, node.getDatacenter(), nodeId);
    }

    private boolean establishConnectionToNode(final Node node)
    {
        UUID nodeId = node.getHostId();
        JMXConnector jmxConnector = myJmxConnectionProvider.getJmxConnector(nodeId);
        boolean isConnected = jmxConnector != null && isConnected(jmxConnector);

        if (isConnected)
        {
            myJmxConnectionProvider.getJmxConnections().put(nodeId, jmxConnector);
            LOG.info("Node {} connected successfully.", nodeId);
        }
        else
        {
            LOG.warn("Failed to connect to node {}.", nodeId);
        }

        return isConnected;
    }

    private boolean isConnected(final JMXConnector jmxConnector)
    {
        try
        {
            jmxConnector.getConnectionId();
            return true;
        }
        catch (IOException e)
        {
            LOG.error("Error while checking connection for JMX connector", e);
            return false;
        }
    }
*/
    @Override
    public void destroy()
    {
        LOG.info("Shutting down RetrySchedulerService...");
        RetryServiceShutdownManager.shutdownExecutorService(myScheduler, DEFAULT_SCHEDULER_AWAIT_TERMINATION_IN_SECONDS, TimeUnit.SECONDS);
        LOG.info("RetrySchedulerService shut down complete.");
    }

    @VisibleForTesting
    ScheduledExecutorService getMyScheduler()
    {
        return myScheduler;
    }
}
