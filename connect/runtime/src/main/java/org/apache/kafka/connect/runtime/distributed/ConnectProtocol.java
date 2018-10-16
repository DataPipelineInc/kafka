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

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the protocol for Kafka Connect workers in a group. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors and tasks to workers.
 */
public class ConnectProtocol {
    public static final String VERSION_KEY_NAME = "version";
    public static final String URL_KEY_NAME = "url";
    public static final String CONFIG_OFFSET_KEY_NAME = "config-offset";
    public static final String CONNECTOR_KEY_NAME = "connector";
    public static final String LEADER_KEY_NAME = "leader";
    public static final String LEADER_URL_KEY_NAME = "leader-url";
    public static final String ERROR_KEY_NAME = "error";
    public static final String TASKS_KEY_NAME = "tasks";
    public static final String ASSIGNMENT_KEY_NAME = "assignment";
    public static final String REVOKED_ASSIGNMENT_KEY_NAME = "revoked-assignment";
    public static final String CONNECTOR_ASSIGNMENTS_KEY_NAME = "resource";
    public static final int CONNECTOR_TASK = -1;

    public static final short CONNECT_PROTOCOL_V0 = 0;
    public static final short CONNECT_PROTOCOL_V1 = 1;
    public static final Schema CONNECT_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16));
    private static final Struct CONNECT_PROTOCOL_HEADER_V0 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONNECT_PROTOCOL_V0);
    private static final Struct CONNECT_PROTOCOL_HEADER_V1 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONNECT_PROTOCOL_V1);

    public static final Schema CONFIG_STATE_V0 = new Schema(
            new Field(URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64));


    // Assignments for each worker are a set of connectors and tasks. These are categorized by connector ID. A sentinel
    // task ID (CONNECTOR_TASK) is used to indicate the connector itself (i.e. that the assignment includes
    // responsibility for running the Connector instance in addition to any tasks it generates).
    public static final Schema CONNECTOR_ASSIGNMENT_V0 = new Schema(
            new Field(CONNECTOR_KEY_NAME, Type.STRING),
            new Field(TASKS_KEY_NAME, new ArrayOf(Type.INT32)));

    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V0)));

    public static final Schema ASSIGNMENT_V1 = new Schema(
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V0)),
            new Field(REVOKED_ASSIGNMENT_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V0)));

    public static final Schema CONFIG_STATE_V1 = new Schema(
            new Field(URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(CONNECTOR_ASSIGNMENTS_KEY_NAME, new ArrayOf(CONNECTOR_ASSIGNMENT_V0)));

    public static ByteBuffer serializeMetadata(WorkerState workerState) {
        Struct struct = new Struct(CONFIG_STATE_V1);
        struct.set(URL_KEY_NAME, workerState.url());
        struct.set(CONFIG_OFFSET_KEY_NAME, workerState.offset());
        struct.set(CONNECTOR_ASSIGNMENTS_KEY_NAME, convertConnectorAssignmentMap(workerState.assignment().asMap()));
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf() + CONFIG_STATE_V1.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        CONFIG_STATE_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static WorkerState deserializeMetadata(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        if (version == CONNECT_PROTOCOL_V0) {
            Struct struct = CONFIG_STATE_V0.read(buffer);
            long configOffset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
            String url = struct.getString(URL_KEY_NAME);
            return new WorkerState(url, configOffset);
        }
        if (version == CONNECT_PROTOCOL_V1) {
            Struct struct = CONFIG_STATE_V1.read(buffer);
            long configOffset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
            String url = struct.getString(URL_KEY_NAME);
            return new WorkerState(url, configOffset, convertToConnectorAssignments(struct, CONNECTOR_ASSIGNMENTS_KEY_NAME));
        }
        throw new SchemaException("Unsupported subscription version: " + version);
    }

    public static ByteBuffer serializeAssignment(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V1);
        struct.set(ERROR_KEY_NAME, assignment.error());
        struct.set(LEADER_KEY_NAME, assignment.leader());
        struct.set(LEADER_URL_KEY_NAME, assignment.leaderUrl());
        struct.set(CONFIG_OFFSET_KEY_NAME, assignment.offset());
        struct.set(ASSIGNMENT_KEY_NAME, convertConnectorAssignmentMap(assignment.asMap()));
        struct.set(REVOKED_ASSIGNMENT_KEY_NAME, convertConnectorAssignmentMap(assignment.revokedAsMap()));
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf() + ASSIGNMENT_V1.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    private static Object[] convertConnectorAssignmentMap(Map<String, List<Integer>> map) {
        List<Struct> structArray = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> connectorEntry : map.entrySet()) {
            Struct taskAssignment = new Struct(CONNECTOR_ASSIGNMENT_V0);
            taskAssignment.set(CONNECTOR_KEY_NAME, connectorEntry.getKey());
            List<Integer> tasks = connectorEntry.getValue();
            taskAssignment.set(TASKS_KEY_NAME, tasks.toArray());
            structArray.add(taskAssignment);
        }
        return structArray.toArray();
    }

    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        if (version == CONNECT_PROTOCOL_V0) {
            Struct struct = ASSIGNMENT_V0.read(buffer);
            short error = struct.getShort(ERROR_KEY_NAME);
            String leader = struct.getString(LEADER_KEY_NAME);
            String leaderUrl = struct.getString(LEADER_URL_KEY_NAME);
            long offset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
            Assignment assignments = convertToConnectorAssignments(struct, ASSIGNMENT_KEY_NAME);
            return new Assignment(error, leader, leaderUrl, offset, assignments.connectorIds, assignments.taskIds);

        }
        if (version == CONNECT_PROTOCOL_V1) {
            Struct struct = ASSIGNMENT_V1.read(buffer);
            short error = struct.getShort(ERROR_KEY_NAME);
            String leader = struct.getString(LEADER_KEY_NAME);
            String leaderUrl = struct.getString(LEADER_URL_KEY_NAME);
            long offset = struct.getLong(CONFIG_OFFSET_KEY_NAME);
            Assignment assignments = convertToConnectorAssignments(struct, ASSIGNMENT_KEY_NAME);
            Assignment revokedAssignments = convertToConnectorAssignments(struct, REVOKED_ASSIGNMENT_KEY_NAME);
            return new Assignment(error, leader, leaderUrl, offset, assignments.connectorIds, assignments.taskIds,
                    revokedAssignments.connectorIds, revokedAssignments.taskIds);
        }
        throw new SchemaException("Unsupported subscription version: " + version);
    }

    private static Assignment convertToConnectorAssignments(Struct struct, String keyName) {
        List<String> connectorIds = new ArrayList<>();
        List<ConnectorTaskId> taskIds = new ArrayList<>();
        for (Object structObj : struct.getArray(keyName)) {
            Struct assignment = (Struct) structObj;
            String connector = assignment.getString(CONNECTOR_KEY_NAME);
            for (Object taskIdObj : assignment.getArray(TASKS_KEY_NAME)) {
                Integer taskId = (Integer) taskIdObj;
                if (taskId == CONNECTOR_TASK) {
                    connectorIds.add(connector);
                } else
                    taskIds.add(new ConnectorTaskId(connector, (Integer) taskIdObj));
            }
        }
        return new Assignment(connectorIds, taskIds);
    }

    public static class WorkerState {
        private final String url;
        private final long offset;
        private final Assignment assignment;

        public WorkerState(String url, long offset) {
            this.url = url;
            this.offset = offset;
            this.assignment = new Assignment(new ArrayList<String>(), new ArrayList<ConnectorTaskId>());
        }

        public WorkerState(String url, long offset, Assignment assignment) {
            this.url = url;
            this.offset = offset;
            this.assignment = assignment;
        }

        public String url() {
            return url;
        }

        public long offset() {
            return offset;
        }

        public Assignment assignment() {
            return assignment;
        }

        @Override
        public String toString() {
            return "WorkerState{" +
                    "url='" + url + '\'' +
                    ", offset=" + offset +
                    ", assignments=" + assignment +
                    '}';
        }
    }

    public static class Assignment {
        public static final short NO_ERROR = 0;
        // Configuration offsets mismatched in a way that the leader could not resolve. Workers should read to the end
        // of the config log and try to re-join
        public static final short CONFIG_MISMATCH = 1;

        private final short error;
        private final String leader;
        private final String leaderUrl;
        private final long offset;
        private final List<String> connectorIds;
        private final List<ConnectorTaskId> taskIds;
        private final List<String> revokedConnectorIds;
        private final List<ConnectorTaskId> revokedTaskIds;

        /**
         * Create an assignment indicating responsibility for the given connector instances and task Ids.
         *
         * @param connectorIds list of connectors that the worker should instantiate and run
         * @param taskIds      list of task IDs that the worker should instantiate and run
         */
        public Assignment(short error, String leader, String leaderUrl, long configOffset,
                          List<String> connectorIds, List<ConnectorTaskId> taskIds) {
            this(error, leader, leaderUrl, configOffset, connectorIds, taskIds, new ArrayList<String>(), new ArrayList<ConnectorTaskId>());
        }

        public Assignment(List<String> connectorIds, List<ConnectorTaskId> taskIds) {
            this(NO_ERROR, null, null, -1, connectorIds, taskIds);
        }

        public Assignment(short error, String leader, String leaderUrl, long configOffset,
                          List<String> connectorIds, List<ConnectorTaskId> taskIds, List<String>
                                  revokedConnectorIds, List<ConnectorTaskId> revokedTaskIds) {
            this.error = error;
            this.leader = leader;
            this.leaderUrl = leaderUrl;
            this.offset = configOffset;
            this.taskIds = taskIds;
            this.connectorIds = connectorIds;
            this.revokedConnectorIds = revokedConnectorIds;
            this.revokedTaskIds = revokedTaskIds;
        }

        public short error() {
            return error;
        }

        public String leader() {
            return leader;
        }

        public String leaderUrl() {
            return leaderUrl;
        }

        public boolean failed() {
            return error != NO_ERROR;
        }

        public long offset() {
            return offset;
        }

        public List<String> connectors() {
            return connectorIds;
        }

        public List<ConnectorTaskId> tasks() {
            return taskIds;
        }

        public List<String> revokedConnectors() {
            return revokedConnectorIds;
        }

        public List<ConnectorTaskId> revokedTasks() {
            return revokedTaskIds;
        }

        @Override
        public String toString() {
            return "Assignment{" +
                    "error=" + error +
                    ", leader='" + leader + '\'' +
                    ", leaderUrl='" + leaderUrl + '\'' +
                    ", offset=" + offset +
                    ", connectorIds=" + connectorIds +
                    ", taskIds=" + taskIds +
                    ", revokedConnectorIds=" + revokedConnectorIds +
                    ", revokedTaskIds=" + revokedTaskIds +
                    '}';
        }

        private Map<String, List<Integer>> asMap() {
            // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
            Map<String, List<Integer>> taskMap = new LinkedHashMap<>();
            for (String connectorId : new HashSet<>(connectorIds)) {
                List<Integer> connectorTasks = taskMap.get(connectorId);
                if (connectorTasks == null) {
                    connectorTasks = new ArrayList<>();
                    taskMap.put(connectorId, connectorTasks);
                }
                connectorTasks.add(CONNECTOR_TASK);
            }
            for (ConnectorTaskId taskId : taskIds) {
                String connectorId = taskId.connector();
                List<Integer> connectorTasks = taskMap.get(connectorId);
                if (connectorTasks == null) {
                    connectorTasks = new ArrayList<>();
                    taskMap.put(connectorId, connectorTasks);
                }
                connectorTasks.add(taskId.task());
            }
            return taskMap;
        }

        private Map<String, List<Integer>> revokedAsMap() {
            // Using LinkedHashMap preserves the ordering, which is helpful for tests and debugging
            Map<String, List<Integer>> taskMap = new LinkedHashMap<>();
            for (String connectorId : new HashSet<>(revokedConnectorIds)) {
                List<Integer> connectorTasks = taskMap.get(connectorId);
                if (connectorTasks == null) {
                    connectorTasks = new ArrayList<>();
                    taskMap.put(connectorId, connectorTasks);
                }
                connectorTasks.add(CONNECTOR_TASK);
            }
            for (ConnectorTaskId taskId : revokedTaskIds) {
                String connectorId = taskId.connector();
                List<Integer> connectorTasks = taskMap.get(connectorId);
                if (connectorTasks == null) {
                    connectorTasks = new ArrayList<>();
                    taskMap.put(connectorId, connectorTasks);
                }
                connectorTasks.add(taskId.task());
            }
            return taskMap;
        }
    }

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONNECT_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed as V0
    }

}
