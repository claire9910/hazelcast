/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.alto;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.actor.ActorRef;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

/**
 * An {@link ActorRef} that routes messages to the {@link PartitionActor}.
 * <p>
 * todo:
 * Should also handle redirect messages.
 */
public final class PartitionActorRef extends ActorRef<IOBuffer> {

    private final int partitionId;
    private final InternalPartitionService partitionService;
    private final Address thisAddress;
    private final Requests requests;
    private final AltoRuntime altoRuntime;
    private final Eventloop eventloop;

    public PartitionActorRef(int partitionId,
                             InternalPartitionService partitionService,
                             TpcEngine engine,
                             AltoRuntime altoRuntime,
                             Address thisAddress,
                             Requests requests) {
        this.partitionId = partitionId;
        this.partitionService = partitionService;
        this.thisAddress = thisAddress;
        this.requests = requests;
        this.altoRuntime = altoRuntime;
        this.eventloop = engine.eventloop(hashToIndex(partitionId, engine.eventloopCount()));
    }

    public RequestFuture<IOBuffer> submit(IOBuffer request) {
        RequestFuture future = new RequestFuture(request);

        //long callId = requests.nextCallId();

        requests.slots.put(future);

        Address address = partitionService.getPartitionOwner(partitionId);
        if (address.equals(thisAddress)) {
            //TODO: deal with return value
            eventloop.offer(request);
        } else {
            // todo: this should in theory not be needed. We could use the last
            // address and only in case of a redirect, we update.
            TcpServerConnection connection = altoRuntime.getConnection(address);

            AsyncSocket socket = connection.sockets[hashToIndex(partitionId, connection.sockets.length)];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();

            //todo: deal with return value.
            socket.writeAndFlush(request);
        }
        return future;
    }

    @Override
    public void send(IOBuffer request) {
        submit(request);
    }
}
