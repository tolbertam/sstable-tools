package com.csforge.sstable;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageConnection {
    private static final Logger logger = LoggerFactory.getLogger(StorageConnection.class);
    private static final String PREFIX = Config.PROPERTY_PREFIX;
    private static final String BUFFER_SIZE_PROPERTY = PREFIX + "otc_buffer_size";
    private static final int BUFFER_SIZE = Integer.getInteger(BUFFER_SIZE_PROPERTY, 1024 * 64);

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    private DataOutputStreamPlus out;
    private Socket socket;
    private volatile int targetVersion;
    private final InetAddress host;
    private static final AtomicInteger idGen = new AtomicInteger(0);

    public StorageConnection(InetAddress host) {
        this.host = host;
        targetVersion = 10;
    }

    public synchronized void enqueue(MessageOut<?> message, int id) throws IOException {
        long timestamp = System.currentTimeMillis();
        out.writeInt(MessagingService.PROTOCOL_MAGIC);

        if (targetVersion < MessagingService.VERSION_20)
            out.writeUTF(String.valueOf(id));
        else
            out.writeInt(id);
        out.writeInt((int) timestamp);
        message.serialize(out, targetVersion);
        out.flush();
    }

    private static void writeHeader(DataOutput out, int version, boolean compressionEnabled) throws IOException {
        int header = 0;
        if (compressionEnabled)
            header |= 4;
        header |= (version << 8);
        out.writeInt(header);
    }

    public boolean connect() {
        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        while (System.nanoTime() - start < timeout) {
            targetVersion = 10;
            try {
                SocketChannel channel = SocketChannel.open();
                channel.connect(new InetSocketAddress(host, DatabaseDescriptor.getStoragePort()));
                socket = channel.socket();
                socket.setTcpNoDelay(true);
                if (DatabaseDescriptor.getInternodeSendBufferSize() != 0) {
                    try {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());
                    } catch (SocketException se) {
                        System.err.println("Failed to set send buffer size on internode socket." + se);
                    }
                }

                // SocketChannel may be null when using SSL
                WritableByteChannel ch = socket.getChannel();
                out = new BufferedDataOutputStreamPlus(ch != null ? ch : Channels.newChannel(socket.getOutputStream()), BUFFER_SIZE);

                out.writeInt(MessagingService.PROTOCOL_MAGIC);
                writeHeader(out, targetVersion, false);
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                int maxTargetVersion = in.readInt();
                MessagingService.instance().setVersion(host, maxTargetVersion);

                out.writeInt(MessagingService.current_version);
                CompactEndpointSerializationHelper.serialize(FBUtilities.getBroadcastAddress(), out);
                out.flush();
                return true;
            } catch (IOException e) {
                socket = null;
                e.printStackTrace();
                System.err.println("unable to connect to " + host + e);
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
        }
        return false;
    }
}
