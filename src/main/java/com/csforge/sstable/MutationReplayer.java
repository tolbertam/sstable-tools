package com.csforge.sstable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MutationReplayer {
    private final Cluster cluster;
    private ConcurrentMap<InetAddress, StorageConnection> connections = Maps.newConcurrentMap();
    private static final AtomicInteger idGen = new AtomicInteger(0);

    public MutationReplayer(Cluster cluster) {
        this.cluster = cluster;
    }

    public void sendMutation(Mutation mutation) {
        for (PartitionUpdate partition : mutation.getPartitionUpdates()) {
            Set<Host> replicas = cluster.getMetadata().getReplicas(mutation.getKeyspaceName(),
                    partition.partitionKey().getKey());
            // in case theres multiple partitions in this mutation, with topology changes we cant assume can send
            // them in batches so break them up.
            Mutation toSend = new Mutation(mutation.getKeyspaceName(), partition.partitionKey());
            toSend.add(partition);
            for(Host h : replicas) {
                InetAddress target = h.getBroadcastAddress();
                StorageConnection conn = connections.get(target);
                if(conn == null) {
                    conn = connections.computeIfAbsent(target, host -> {
                        StorageConnection c = new StorageConnection(host);
                        c.connect();
                        return c;
                    });
                }
                try {
                    conn.enqueue(toSend.createMessage(), idGen.incrementAndGet());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
