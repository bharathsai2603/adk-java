/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.adk.runner;

import com.google.adk.agents.BaseAgent;
import com.google.adk.artifacts.CassandraArtifactService;
import com.google.adk.memory.CassandraMemoryService;
import com.google.adk.memory.RedbusEmbeddingService;
import com.google.adk.plugins.BasePlugin;
import com.google.adk.sessions.CassandraSessionService;
import com.google.adk.utils.CassandraDBHelper;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * The class for the Cassandra-backed GenAI runner.
 *
 * <p>This runner uses CassandraDBHelper as a singleton connection manager. Configuration is loaded
 * from environment variables with sensible defaults:
 *
 * <ul>
 *   <li>CASSANDRA_HOST - Cassandra host (default: localhost)
 *   <li>CASSANDRA_PORT - Cassandra port (default: 9042)
 *   <li>CASSANDRA_USER - Cassandra username (default: cassandra)
 *   <li>CASSANDRA_PASSWORD - Cassandra password (default: cassandra)
 *   <li>CASSANDRA_KEYSPACE - Cassandra keyspace (default: rae)
 *   <li>CASSANDRA_DATACENTER - Cassandra datacenter (default: datacenter1)
 *   <li>CASSANDRA_REQUEST_TIMEOUT - Request timeout in seconds (default: 5)
 * </ul>
 *
 * <p>All configuration has defaults, so for local development with a default Cassandra setup, no
 * environment variables need to be set.
 *
 * @author Sandeep Belgavi
 * @since 2025-10-19
 */
public class CassandraRunner extends Runner {

  /**
   * Initializes the runner with the given agent.
   *
   * <p>Uses the agent's name as the app name and an empty plugin list. CassandraDBHelper is
   * automatically initialized from environment variables (or defaults).
   *
   * @param agent the agent to run
   */
  public CassandraRunner(BaseAgent agent) {
    this(agent, agent.name(), ImmutableList.of());
  }

  /**
   * Initializes the runner with the given agent and app name.
   *
   * <p>Uses an empty plugin list. CassandraDBHelper is automatically initialized from environment
   * variables (or defaults).
   *
   * @param agent the agent to run
   * @param appName the name of the application
   */
  public CassandraRunner(BaseAgent agent, String appName) {
    this(agent, appName, ImmutableList.of());
  }

  /**
   * Initializes the runner with the given agent, app name, and plugins.
   *
   * <p>CassandraDBHelper is automatically initialized from environment variables (or defaults). All
   * services (session, artifact, memory) use the singleton CassandraDBHelper connection. A 5-second
   * request timeout is configured by default.
   *
   * @param agent the agent to run
   * @param appName the name of the application
   * @param plugins the list of plugins to use
   */
  public CassandraRunner(BaseAgent agent, String appName, List<BasePlugin> plugins) {
    super(
        agent,
        appName,
        new CassandraArtifactService(),
        new CassandraSessionService(),
        new CassandraMemoryService(
            "rae",
            "rae_data",
            new RedbusEmbeddingService(
                System.getenv("ADU") != null ? System.getenv("ADU") : "",
                System.getenv("ADP") != null ? System.getenv("ADP") : "")),
        plugins);

    // Initialize CassandraDBHelper singleton (lazy initialization on first access)
    CassandraDBHelper.getInstance();
  }
}
