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

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;

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
    }

    @Override
    public void requestRejoin() {
        rejoinRequested = true;
    }

    @Override
    public String protocolType() {
        return "connect";
    }

    // expose for tests
    @Override
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        return super.ensureCoordinatorReady(timer);
    }

    public void poll(long timeout) {
        // poll for io until the timeout expires
        final long start = time.milliseconds();
        long now = start;
        long remaining;

        do {
            if (coordinatorUnknown()) {
                ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
                now = time.milliseconds();
            }

            if (rejoinNeededOrPending()) {
                ensureActiveGroup();
                now = time.milliseconds();
            }

            pollHeartbeat(now);

            long elapsed = now - start;
            remaining = timeout - elapsed;

            // Note that because the network client is shared with the background heartbeat thread,
            // we do not want to block in poll longer than the time to the next heartbeat.
            long pollTimeout = Math.min(Math.max(0, remaining), timeToNextHeartbeat(now));
            client.poll(time.timer(pollTimeout));

            now = time.milliseconds();
            elapsed = now - start;
            remaining = timeout - elapsed;
        } while (remaining > 0);
    }


    @Override
    public List<ProtocolMetadata> metadata() {
        configSnapshot = configStorage.snapshot();
        ConnectProtocol.WorkerState workerState = new ConnectProtocol.WorkerState(restUrl,
                configSnapshot.offset(),
                assignmentSnapshot == null ?
                        new ConnectProtocol.Assignment(new ArrayList<String>(), new ArrayList<ConnectorTaskId>()) : assignmentSnapshot);
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        return Collections.singletonList(new ProtocolMetadata(DEFAULT_SUBPROTOCOL, metadata));
    }

    @Override
    protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
        ConnectProtocol.Assignment assignment = ConnectProtocol.deserializeAssignment(memberAssignment);
        assignmentSnapshot = new ConnectProtocol.Assignment(
                assignment.error(),
                assignment.leader(),
                assignment.leaderUrl(),
                assignment.offset(),
                assignmentSnapshot == null ? new ArrayList<String>() : assignmentSnapshot.connectors(),
                assignmentSnapshot == null ? new ArrayList<ConnectorTaskId>() :
                        assignmentSnapshot.tasks());
        assignmentSnapshot.connectors().addAll(assignment.connectors());
        assignmentSnapshot.tasks().addAll(assignment.tasks());
        assignmentSnapshot.connectors().removeAll(assignment.revokedConnectors());
        assignmentSnapshot.tasks().removeAll(assignment.revokedTasks());
        // At this point we always consider ourselves to be a member of the cluster, even if there was an assignment
        // error (the leader couldn't make the assignment) or we are behind the config and cannot yet work on our assigned
        // tasks. It's the responsibility of the code driving this process to decide how to react (e.g. trying to get
        // up to date, try to rejoin again, leaving the group and backing off, etc.).
        rejoinRequested = false;
        listener.onAssigned(assignment, generation);
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
                    new HashMap<String, List<String>>(),
                    new HashMap<String, List<ConnectorTaskId>>(),
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


    private class AssignmentResult<T> {
        final Map<String, List<T>> revokedTs = new HashMap<>();
        final Map<String, List<T>> addedTs = new HashMap<>();
        final Map<String, List<T>> finalTs = new HashMap<>();

        AssignmentResult(List<String> memberIds) {
            for (String memberId : memberIds) {
                revokedTs.put(memberId, new ArrayList<T>());
                addedTs.put(memberId, new ArrayList<T>());
                finalTs.put(memberId, new ArrayList<T>());
            }
        }
    }

    private <T> AssignmentResult<T> compute(List<String> memberIds, List<T> allTs, Map<String, List<T>> lastTsOfAll) {
        Map<String, Integer> targetCounts = new HashMap<>();
        for (String memberId : memberIds) {
            targetCounts.put(memberId, 0);
        }
        for (int i = 0; i < allTs.size(); i++) {
            String memberId = Iterables.get(memberIds, (i % memberIds.size()));
            targetCounts.put(memberId, targetCounts.get(memberId) + 1);
        }
        Map<T, String> tOwners = invertAssignment(lastTsOfAll);
        List<T> newTs = new ArrayList<>(allTs);
        newTs.removeAll(tOwners.keySet());
        AssignmentResult<T> result = new AssignmentResult<>(memberIds);
        for (String id : memberIds) {
            int targetCount = targetCounts.get(id);
            int count = 0;
            List<T> lastTs = lastTsOfAll.get(id);
            for (T t : lastTs) {
                if (allTs.contains(t) && count < targetCount) {
                    result.finalTs.get(id).add(t);
                    count++;
                } else {
                    result.revokedTs.get(id).add(t);
                }
            }

            for (Iterator<T> it = newTs.iterator(); it.hasNext(); ) {
                T t = it.next();
                if (count < targetCount) {
                    result.finalTs.get(id).add(t);
                    count++;
                    result.addedTs.get(id).add(t);
                    it.remove();
                }
            }

        }
        return result;
    }


    private Map<String, ByteBuffer> performTaskAssignment(String leaderId, long maxOffset, Map<String, ConnectProtocol.WorkerState> memberConfigs) {
        List<String> currentAllConnectors = sorted(configSnapshot.connectors());
        Set<ConnectorTaskId> currAllTasksSet = new HashSet<>();
        for (String connector : configSnapshot.connectors()) {
            currAllTasksSet.addAll(configSnapshot.tasks(connector));
        }
        List<ConnectorTaskId> currentAllTasks = sorted(currAllTasksSet);

        Map<String, List<String>> lastConnectors = new HashMap<>();
        Map<String, List<ConnectorTaskId>> lastTasks = new HashMap<>();

        for (Map.Entry<String, ConnectProtocol.WorkerState> entry : memberConfigs.entrySet()) {
            String memberId = entry.getKey();
            ConnectProtocol.Assignment lastAssignment = entry.getValue().assignment();
            lastConnectors.put(memberId, lastAssignment.connectors());
            lastTasks.put(memberId, lastAssignment.tasks());
        }

        List<String> memberIds = sorted(memberConfigs.keySet());

        AssignmentResult<String> connectorResult = compute(memberIds, currentAllConnectors, lastConnectors);
        AssignmentResult<ConnectorTaskId> taskResult = compute(memberIds, currentAllTasks, lastTasks);


        this.leaderState = new LeaderState(memberConfigs, connectorResult.finalTs, taskResult.finalTs);

        return fillAssignmentsAndSerialize(memberConfigs.keySet(), ConnectProtocol.Assignment.NO_ERROR,
                leaderId, memberConfigs.get(leaderId).url(), maxOffset,
                connectorResult.addedTs,
                taskResult.addedTs,
                connectorResult.revokedTs,
                taskResult.revokedTs);
    }

    private Map<String, ByteBuffer> fillAssignmentsAndSerialize(Collection<String> members,
                                                                short error,
                                                                String leaderId,
                                                                String leaderUrl,
                                                                long maxOffset,
                                                                Map<String, List<String>> connectorAssignments,
                                                                Map<String, List<ConnectorTaskId>> taskAssignments,
                                                                Map<String, List<String>> revokedConnectorAssignments,
                                                                Map<String, List<ConnectorTaskId>> revokedTaskAssignments) {

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
/*        log.debug("Revoking previous assignment {}", assignmentSnapshot);
        if (assignmentSnapshot != null && !assignmentSnapshot.failed())
            listener.onRevoked(assignmentSnapshot.leader(), assignmentSnapshot.connectors(), assignmentSnapshot.tasks());*/
    }

    @Override
    protected boolean rejoinNeededOrPending() {
        return super.rejoinNeededOrPending() || (assignmentSnapshot == null || assignmentSnapshot.failed()) || rejoinRequested;
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
        if (rejoinNeededOrPending() || !isLeader())
            return null;
        return leaderState.ownerUrl(connector);
    }

    public String ownerUrl(ConnectorTaskId task) {
        if (rejoinNeededOrPending() || !isLeader())
            return null;
        return leaderState.ownerUrl(task);
    }

    private class WorkerCoordinatorMetrics {
        public final String metricGrpName;

        public WorkerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            Measurable numConnectors = new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return assignmentSnapshot.connectors().size();
                }
            };

            Measurable numTasks = new Measurable() {
                @Override
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
