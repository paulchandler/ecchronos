package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/***
 * Dummy node used for testing, only endPoint, listenAddress and hostId are set.
 */

public class DummyNode implements Node {

    EndPoint endPoint;
    InetSocketAddress listenAddress;

    UUID hostId;
    public DummyNode(EndPoint endPoint, InetSocketAddress listenAddress, UUID hostId){
        this.endPoint = endPoint;
        this.listenAddress = listenAddress;
        this.hostId = hostId;
    }
    @Override
    public EndPoint getEndPoint() {
        return endPoint;
    }

    @Override
    public Optional<InetSocketAddress> getBroadcastRpcAddress() {
        return Optional.empty();
    }

    @Override
    public Optional<InetSocketAddress> getBroadcastAddress() {
        return Optional.empty();
    }

    @Override
    public Optional<InetSocketAddress> getListenAddress() {
        return Optional.ofNullable(listenAddress);
    }

    public void setListenAddress(InetSocketAddress listenAddress){
        this.listenAddress = listenAddress;
    }

    @Override
    public String getDatacenter() {
        return null;
    }

    @Override
    public String getRack() {
        return null;
    }

    @Override
    public Version getCassandraVersion() {
        return null;
    }

    @Override
    public Map<String, Object> getExtras() {
        return null;
    }

    @Override
    public NodeState getState() {
        return null;
    }

    @Override
    public long getUpSinceMillis() {
        return 0;
    }

    @Override
    public int getOpenConnections() {
        return 0;
    }

    @Override
    public boolean isReconnecting() {
        return false;
    }

    @Override
    public NodeDistance getDistance() {
        return null;
    }

    @Override
    public UUID getHostId() {
        return hostId;
    }

    @Override
    public UUID getSchemaVersion() {
        return null;
    }
}