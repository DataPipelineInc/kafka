/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the coordination process with the Kafka group coordinator on the broker for managing assignments
 * to workers.
 */
public final class WorkerCoordinator extends AbstractCoordinator implements Closeable {
    // Currently doesn't support multiple task assignment strategies, so we just fill in a default value
    public static final String DEFAULT_SUBPROTOCOL = "default";

    private final Logger log;
    private final String restUrl;
    private final ConfigBackingStore configStorage;
    private ConnectProtocol.Assignment assignmentSnapshot;
    private ClusterConfigState configSnapshot;
    private final WorkerRebalanceListener listener;
    private LeaderState leaderState;
    private ConnectProtocol.ConnectorAssignments connectorAssignments;

    private boolean rejoinRequested;

    /**
     * Initialize the coordination manager.
     */
    public WorkerCoordinator(LogContext logContext,
                             ConsumerNetworkClient client,
                             String groupId,
                             int rebalanceTimeoutMs,
                             int sessionTimeoutMs,
                             int heartbeatIntervalMs,
                             Metrics metrics,
                             String metricGrpPrefix,
                             Time time,
                             long retryBackoffMs,
                             String restUrl,
                             ConfigBackingStore configStorage,
                             WorkerRebalanceListener listener) {
        super(logContext,
              client,
              groupId,
              rebalanceTimeoutMs,
              sessionTimeoutMs,
              heartbeatIntervalMs,
              metrics,
              metricGrpPrefix,
              time,
              retryBackoffMs,
              true);
        this.log = logContext.logger(WorkerCoordinator.class);
        this.restUrl = restUrl;
        this.configStorage = configStorage;
        this.assignmentSnapshot = null;
        new WorkerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.listener = listener;
        this.rejoinRequested = false;
        this.connectorAssignments = new ConnectProtocol.ConnectorAssignments();
    }

    public void requestRejoin() {
        rejoinRequested = true;
    }

    @Override
    public String protocolType() {
        return "connect";
    }

    public void poll(long timeout) {
        // poll for io until the timeout expires
        final long start = time.milliseconds();
        long now = start;
        long remaining;

        do {
            if (coordinatorUnknown()) {
                ensureCoordinatorReady();
                now = time.milliseconds();
            }

            if (needRejoin()) {
                ensureActiveGroup();
                now = time.milliseconds();
            }

            pollHeartbeat(now);

            long elapsed = now - start;
            remaining = timeout - elapsed;

            // Note that because the network client is shared with the background heartbeat thread,
            // we do not want to block in poll longer than the time to the next heartbeat.
            client.poll(Math.min(Math.max(0, remaining), timeToNextHeartbeat(now)));

            now = time.milliseconds();
            elapsed = now - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);
    }


    @Override
    public List<ProtocolMetadata> metadata() {
        configSnapshot = configStorage.snapshot();
        ConnectProtocol.WorkerState workerState = new ConnectProtocol.WorkerState(restUrl, configSnapshot.offset(), connectorAssignments);
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        return Collections.singletonList(new ProtocolMetadata(DEFAULT_SUBPROTOCOL, metadata));
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        assignmentSnapshot = ConnectProtocol.deserializeAssignment(memberAssignment);
        // At this point we always consider ourselves to be a member of the cluster, even if there was an assignment
        // error (the leader couldn't make the assignment) or we are behind the config and cannot yet work on our assigned
        // tasks. It's the responsibility of the code driving this process to decide how to react (e.g. trying to get
        // up to date, try to rejoin again, leaving the group and backing off, etc.).
        rejoinRequested = false;
        listener.onAssigned(assignmentSnapshot, generation);
        connectorAssignments.connectors().addAll(assignmentSnapshot.connectors());
        connectorAssignments.connectors().removeAll(assignmentSnapshot.revokedConnectors());
        connectorAssignments.tasks().addAll(assignmentSnapshot.tasks());
        connectorAssignments.tasks().removeAll(assignmentSnapshot.revokedTasks());
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId, String protocol, Map<String, ByteBuffer> allMemberMetadata) {
        log.debug("Performing task assignment");

        Map<String, ConnectProtocol.WorkerState> memberConfigs = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> entry : allMemberMetadata.entrySet())
            memberConfigs.put(entry.getKey(), ConnectProtocol.deserializeMetadata(entry.getValue()));

        long maxOffset = findMaxMemberConfigOffset(memberConfigs);
        Long leaderOffset = ensureLeaderConfig(maxOffset);
        if (leaderOffset == null)
            return fillAssignmentsAndSerialize(memberConfigs.keySet(), ConnectProtocol.Assignment.CONFIG_MISMATCH,
                    leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                    new HashMap<String, List<String>>(), new HashMap<String, List<ConnectorTaskId>>(),
                    new HashMap<String, List<String>>(),
                    new HashMap<String, List<ConnectorTaskId>>());
        return performTaskAssignment(leaderId, leaderOffset, memberConfigs);
    }

    private long findMaxMemberConfigOffset(Map<String, ConnectProtocol.WorkerState> memberConfigs) {
        // The new config offset is the maximum seen by any member. We always perform assignment using this offset,
        // even if some members have fallen behind. The config offset used to generate the assignment is included in
        // the response so members that have fallen behind will not use the assignment until they have caught up.
        Long maxOffset = null;
        for (Map.Entry<String, ConnectProtocol.WorkerState> stateEntry : memberConfigs.entrySet()) {
            long memberRootOffset = stateEntry.getValue().offset();
            if (maxOffset == null)
                maxOffset = memberRootOffset;
            else
                maxOffset = Math.max(maxOffset, memberRootOffset);
        }

        log.debug("Max config offset root: {}, local snapshot config offsets root: {}",
                  maxOffset, configSnapshot.offset());
        return maxOffset;
    }

    private Long ensureLeaderConfig(long maxOffset) {
        // If this leader is behind some other members, we can't do assignment
        if (configSnapshot.offset() < maxOffset) {
            // We might be able to take a new snapshot to catch up immediately and avoid another round of syncing here.
            // Alternatively, if this node has already passed the maximum reported by any other member of the group, it
            // is also safe to use this newer state.
            ClusterConfigState updatedSnapshot = configStorage.snapshot();
            if (updatedSnapshot.offset() < maxOffset) {
                log.info("Was selected to perform assignments, but do not have latest config found in sync request. " +
                        "Returning an empty configuration to trigger re-sync.");
                return null;
            } else {
                configSnapshot = updatedSnapshot;
                return configSnapshot.offset();
            }
        }

        return maxOffset;
    }


    private class AssignState<T> {
        List<T> added = new ArrayList<>();
        List<T> removed = new ArrayList<>();
        List<T> current = new ArrayList<>();
    }

    private <T> AssignState<T> removeNonExists(List<T> last, List<T> all) {
        AssignState<T> assignState = new AssignState<>();
        for (T t : last) {
            if (all.contains(t)) {
                assignState.current.add(t);
            } else {
                assignState.removed.add(t);
            }
        }
        return assignState;
    }

    private <T> void assignForNewConfig(List<T> current, List<T> last, Collection<AssignState<T>> all) {
        for (T t : current) {
            if (!last.contains(t)) {
                AssignState<T> smallest = all.iterator().next();
                for (AssignState<T> as : all) {
                    if (as.current.size() < smallest.current.size()) {
                        smallest = as;
                    }
                }
                smallest.added.add(t);
                smallest.current.add(t);
            }
        }
    }


    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset, Map<String, ConnectProtocol.WorkerState> memberConfigs) {
        List<String> currentAllConnectors = sorted(configSnapshot.connectors());
        Set<ConnectorTaskId> currAllTasksSet = new HashSet<>();
        for (String connector : configSnapshot.connectors()) {
            currAllTasksSet.addAll(configSnapshot.tasks(connector));
        }
        List<ConnectorTaskId> currentAllTasks = sorted(currAllTasksSet);

        List<String> lastAllConnectors = new ArrayList<>();
        List<ConnectorTaskId> lastAllTasks = new ArrayList<>();
        for (ConnectProtocol.WorkerState ws : memberConfigs.values()) {
            lastAllConnectors.addAll(ws.connectorAssignments().connectors());
            lastAllTasks.addAll(ws.connectorAssignments().tasks());
        }
        Map<String, AssignState<String>> connectorsState = new HashMap<>();
        Map<String, AssignState<ConnectorTaskId>> tasksState = new HashMap<>();
        for (Map.Entry<String, ConnectProtocol.WorkerState> entry : memberConfigs.entrySet()) {
            String memberId = entry.getKey();
            ConnectProtocol.ConnectorAssignments lastConnectorAssignments = entry.getValue().connectorAssignments();
            connectorsState.put(memberId, removeNonExists(lastConnectorAssignments.connectors(), currentAllConnectors));
            tasksState.put(memberId, removeNonExists(lastConnectorAssignments.tasks(), currentAllTasks));
        }
        // assign new added connectors and tasks
        assignForNewConfig(currentAllConnectors, lastAllConnectors, connectorsState.values());
        assignForNewConfig(currentAllTasks, lastAllTasks, tasksState.values());


        Map<String, List<String>> assignConnectors = new HashMap<>();
        Map<String, List<String>> addedConnectors = new HashMap<>();
        Map<String, List<String>> revokedConnectors = new HashMap<>();
        Map<String, List<ConnectorTaskId>> assignTasks = new HashMap<>();
        Map<String, List<ConnectorTaskId>> addedTasks = new HashMap<>();
        Map<String, List<ConnectorTaskId>> revokedTasks = new HashMap<>();

        for (Map.Entry<String, AssignState<String>> entry : connectorsState.entrySet()) {
            assignConnectors.put(entry.getKey(), entry.getValue().current);
            addedConnectors.put(entry.getKey(), entry.getValue().added);
            revokedConnectors.put(entry.getKey(), entry.getValue().removed);
        }

        for (Map.Entry<String, AssignState<ConnectorTaskId>> entry : tasksState.entrySet()) {
            assignTasks.put(entry.getKey(), entry.getValue().current);
            addedTasks.put(entry.getKey(), entry.getValue().added);
            revokedTasks.put(entry.getKey(), entry.getValue().removed);
        }

        this.leaderState = new LeaderState(memberConfigs, assignConnectors, assignTasks);

        return fillAssignmentsAndSerialize(memberConfigs.keySet(), ConnectProtocol.Assignment.NO_ERROR,
                                           leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                                           addedConnectors,
                                           addedTasks,
                                           revokedConnectors,
                                           revokedTasks);
    }

    private Map<String, ByteBuffer> fillAssignmentsAndSerialize(Collection<String> members,
                                                                short error,
                                                                String leaderId,
                                                                String leaderUrl,
                                                                long maxOffset,
                                                                Map<String, List<String>> connectorAssignments,
                                                                Map<String,
                                                                        List<ConnectorTaskId>> taskAssignments,
                                                                Map<String, List<String>>
                                                                        revokedConnectorAssignments,
                                                                Map<String,
                                                                        List<ConnectorTaskId>>
                                                                        revokedTaskAssignments) {

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (String member : members) {
            List<String> connectors = connectorAssignments.get(member);
            List<String> revokedConnectors = revokedConnectorAssignments.get(member);
            if (connectors == null)
                connectors = Collections.emptyList();
            if (revokedConnectors == null)
                revokedConnectors = Collections.emptyList();
            List<ConnectorTaskId> tasks = taskAssignments.get(member);
            List<ConnectorTaskId> revokedTasks = revokedTaskAssignments.get(member);
            if (tasks == null)
                tasks = Collections.emptyList();
            if (revokedTasks == null)
                tasks = Collections.emptyList();
            ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(
                    error,
                    leaderId,
                    leaderUrl,
                    maxOffset,
                    connectors,
                    tasks,
                    revokedConnectors,
                    revokedTasks);
            log.debug("Assignment: {} -> {}", member, assignment);
            groupAssignment.put(member, ConnectProtocol.serializeAssignment(assignment));
        }
        log.debug("Finished assignment");
        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        this.leaderState = null;
        log.debug("Revoking previous assignment {}", assignmentSnapshot);
        if (assignmentSnapshot != null && !assignmentSnapshot.failed())
            listener.onRevoked(assignmentSnapshot.leader(), assignmentSnapshot.connectors(), assignmentSnapshot.tasks());
    }

    @Override
    protected boolean needRejoin() {
        return super.needRejoin() || (assignmentSnapshot == null || assignmentSnapshot.failed()) || rejoinRequested;
    }

    public String memberId() {
        Generation generation = generation();
        if (generation != null)
            return generation.memberId;
        return JoinGroupRequest.UNKNOWN_MEMBER_ID;
    }

    private boolean isLeader() {
        return assignmentSnapshot != null && memberId().equals(assignmentSnapshot.leader());
    }

    public String ownerUrl(String connector) {
        if (needRejoin() || !isLeader())
            return null;
        return leaderState.ownerUrl(connector);
    }

    public String ownerUrl(ConnectorTaskId task) {
        if (needRejoin() || !isLeader())
            return null;
        return leaderState.ownerUrl(task);
    }

    private class WorkerCoordinatorMetrics {
        public final String metricGrpName;

        public WorkerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            Measurable numConnectors = new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.connectors().size();
                }
            };

            Measurable numTasks = new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.tasks().size();
                }
            };

            metrics.addMetric(metrics.metricName(
                    "assigned-connectors",
                    this.metricGrpName,
                    "The number of connector instances currently assigned to this consumer"), numConnectors);
            metrics.addMetric(metrics.metricName(
                    "assigned-tasks",
                    this.metricGrpName,
                    "The number of tasks currently assigned to this consumer"), numTasks);
        }
    }

    private static <T extends Comparable<T>> List<T> sorted(Collection<T> members) {
        List<T> res = new ArrayList<>(members);
        Collections.sort(res);
        return res;
    }

    private static <K, V> Map<V, K> invertAssignment(Map<K, List<V>> assignment) {
        Map<V, K> inverted = new HashMap<>();
        for (Map.Entry<K, List<V>> assignmentEntry : assignment.entrySet()) {
            K key = assignmentEntry.getKey();
            for (V value : assignmentEntry.getValue())
                inverted.put(value, key);
        }
        return inverted;
    }

    private static class LeaderState {
        private final Map<String, ConnectProtocol.WorkerState> allMembers;
        private final Map<String, String> connectorOwners;
        private final Map<ConnectorTaskId, String> taskOwners;

        public LeaderState(Map<String, ConnectProtocol.WorkerState> allMembers,
                           Map<String, List<String>> connectorAssignment,
                           Map<String, List<ConnectorTaskId>> taskAssignment) {
            this.allMembers = allMembers;
            this.connectorOwners = invertAssignment(connectorAssignment);
            this.taskOwners = invertAssignment(taskAssignment);
        }

        private String ownerUrl(ConnectorTaskId id) {
            String ownerId = taskOwners.get(id);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

        private String ownerUrl(String connector) {
            String ownerId = connectorOwners.get(connector);
            if (ownerId == null)
                return null;
            return allMembers.get(ownerId).url();
        }

    }

}
