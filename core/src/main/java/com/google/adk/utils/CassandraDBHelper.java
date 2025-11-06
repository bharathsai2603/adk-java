package com.google.adk.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.adk.events.Event;
import com.google.adk.sessions.Session;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Blob;
import com.google.genai.types.FileData;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionResponse;
import com.google.genai.types.Part;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton helper class for managing Cassandra database connections and operations.
 *
 * <p>This class provides a centralized way to manage CqlSession and ObjectMapper instances for all
 * Cassandra-related operations in the application.
 *
 * @author Sandeep Belgavi
 */
public class CassandraDBHelper {
  private static final Logger logger = LoggerFactory.getLogger(CassandraDBHelper.class);
  private static volatile CassandraDBHelper instance;
  private final CqlSession session;
  private final ObjectMapper objectMapper;

  private static final String CASSANDRA_CONTACT_POINTS = "CASSANDRA_HOST";
  private static final String CASSANDRA_PORT = "CASSANDRA_PORT";
  private static final String CASSANDRA_USERNAME = "CASSANDRA_USERNAME";
  private static final String CASSANDRA_PASSWORD = "CASSANDRA_PASSWORD";
  private static final String CASSANDRA_KEYSPACE = "CASSANDRA_KEYSPACE";
  private static final String CASSANDRA_DATACENTER = "CASSANDRA_DATACENTER";
  private static final String CASSANDRA_REQUEST_TIMEOUT_SECONDS =
      "CASSANDRA_REQUEST_TIMEOUT_SECONDS";
  private static final String CASSANDRA_MAX_CONNECTIONS_PER_HOST =
      "CASSANDRA_MAX_CONNECTIONS_PER_HOST";
  // Default values
  private static final String DEFAULT_CONTACT_POINTS = "localhost";
  private static final String DEFAULT_PORT = "9042"; // default port for Cassandra
  private static final String DEFAULT_USERNAME = "cassandra";
  private static final String DEFAULT_PASSWORD = "cassandra";
  private static final String DEFAULT_KEYSPACE = "rae";
  private static final String DEFAULT_DATACENTER = "datacenter1";
  private static final String DEFAULT_REQUEST_TIMEOUT_SECONDS =
      "5"; // default request timeout in seconds
  private static final String DEFAULT_MAX_CONNECTIONS_PER_HOST =
      "32"; // default max connections per host

  private CassandraDBHelper() {
    this.objectMapper = createObjectMapper();
    this.session = initializeCqlSession();
    logger.info("CassandraDBHelper initialized successfully");
  }

  /**
   * Creates and configures the ObjectMapper with necessary modules.
   *
   * @return configured ObjectMapper instance
   */
  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new GuavaModule());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    mapper.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false);
    return mapper;
  }

  /**
   * Returns the singleton instance of CassandraDBHelper.
   *
   * @return the singleton instance
   */
  public static CassandraDBHelper getInstance() {
    if (instance == null) {
      synchronized (CassandraDBHelper.class) {
        if (instance == null) {
          instance = new CassandraDBHelper();
        }
      }
    }
    return instance;
  }

  /**
   * Initializes the Cassandra CQL session from environment variables with defaults.
   *
   * <p>Environment variables:
   *
   * <ul>
   *   <li>CASSANDRA_HOST (default: localhost)
   *   <li>CASSANDRA_PORT (default: 9042)
   *   <li>CASSANDRA_USER (default: cassandra)
   *   <li>CASSANDRA_PASSWORD (default: cassandra)
   *   <li>CASSANDRA_KEYSPACE (default: rae)
   *   <li>CASSANDRA_DATACENTER (default: datacenter1)
   *   <li>CASSANDRA_REQUEST_TIMEOUT (default: 5 seconds)
   * </ul>
   *
   * @return configured CqlSession
   * @throws IllegalArgumentException if port or timeout values are invalid
   */
  private CqlSession initializeCqlSession() {
    String contactPoints = getEnvOrDefault(CASSANDRA_CONTACT_POINTS, DEFAULT_CONTACT_POINTS);
    String username = getEnvOrDefault(CASSANDRA_USERNAME, DEFAULT_USERNAME);
    String password = getEnvOrDefault(CASSANDRA_PASSWORD, DEFAULT_PASSWORD);
    String keyspace = getEnvOrDefault(CASSANDRA_KEYSPACE, DEFAULT_KEYSPACE);
    String datacenter = getEnvOrDefault(CASSANDRA_DATACENTER, DEFAULT_DATACENTER);
    String timeoutStr =
        getEnvOrDefault(CASSANDRA_REQUEST_TIMEOUT_SECONDS, DEFAULT_REQUEST_TIMEOUT_SECONDS);
    String maxConnectionsPerHostStr =
        getEnvOrDefault(CASSANDRA_MAX_CONNECTIONS_PER_HOST, DEFAULT_MAX_CONNECTIONS_PER_HOST);
    int timeoutSeconds;
    try {
      timeoutSeconds = Integer.parseInt(timeoutStr);
    } catch (NumberFormatException e) {
      logger.warn(
          "Invalid CASSANDRA_REQUEST_TIMEOUT: {}, using default: {} seconds",
          timeoutStr,
          DEFAULT_REQUEST_TIMEOUT_SECONDS);
      timeoutSeconds = Integer.parseInt(DEFAULT_REQUEST_TIMEOUT_SECONDS);
    }

    logger.info(
        "Connecting to Cassandra at {}:{} with keyspace: {}, datacenter: {}, timeout: {}s",
        contactPoints,
        keyspace,
        datacenter,
        timeoutSeconds);

    // Configure request timeout programmatically
    // ProgrammaticDriverConfigLoaderBuilder configBuilder =
    // DriverConfigLoader.programmaticBuilder();
    // configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
    // Duration.ofSeconds(timeoutSeconds));
    // configBuilder.withInt(DefaultDriverOption.CONNECTIONS_PER_HOST,
    // Integer.parseInt(maxConnectionsPerHostStr));

    // DriverConfigLoader configLoader = configBuilder.build();

    List<InetSocketAddress> contactPointsList = contactPointsParser(contactPoints);

    CqlSessionBuilder sessionBuilder =
        CqlSession.builder()
            .addContactPoints(contactPointsList)
            .withAuthCredentials(username, password)
            .withKeyspace(keyspace)
            .withLocalDatacenter(datacenter)
            .withConfigLoader(
                buildConfigLoader(
                    Integer.parseInt(maxConnectionsPerHostStr),
                    Duration.ofSeconds(timeoutSeconds)));

    return sessionBuilder.build();
  }

  /**
   * Builds the driver configuration with custom settings.
   *
   * @param maxConnectionsPerHost max connections per host
   * @param requestTimeout request timeout
   * @return configured driver config loader
   */
  private DriverConfigLoader buildConfigLoader(int maxConnectionsPerHost, Duration requestTimeout) {
    return DriverConfigLoader.programmaticBuilder()
        // Connection pool settings
        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, maxConnectionsPerHost)
        .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, maxConnectionsPerHost / 2)

        // Timeout settings
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, requestTimeout)
        .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
        .withDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, Duration.ofSeconds(5))

        // Reconnection settings
        .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1))
        .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(60))

        // Heartbeat settings
        .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(30))
        .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofSeconds(10))

        // Request throttling (prevent overwhelming the cluster)
        .withInt(
            DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS,
            maxConnectionsPerHost * 1024)
        .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, maxConnectionsPerHost * 1024)
        .build();
  }

  /**
   * Gets an environment variable value or returns the default.
   *
   * @param envVar the environment variable name
   * @param defaultValue the default value if not found
   * @return the environment variable value or default
   */
  private String getEnvOrDefault(String envVar, String defaultValue) {
    String value = System.getenv(envVar);
    if (value == null || value.trim().isEmpty()) {
      logger.debug("Environment variable {} not found, using default: {}", envVar, defaultValue);
      return defaultValue;
    }
    return value;
  }

  /**
   * Returns the shared CqlSession instance.
   *
   * @return the CqlSession
   */
  public CqlSession getSession() {
    return session;
  }

  /**
   * Returns the shared ObjectMapper instance.
   *
   * @return the ObjectMapper
   */
  public ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  // --- Session Service Methods ---

  public void saveSession(Session sessionObj) throws JsonProcessingException {
    String insertCql =
        "INSERT INTO sessions (app_name, user_id, session_id, state, events, last_update_time) VALUES (?, ?, ?, ?, ?, ?)";
    session.execute(
        insertCql,
        sessionObj.appName(),
        sessionObj.userId(),
        sessionObj.id(),
        objectMapper.writeValueAsString(sessionObj.state()),
        objectMapper.writeValueAsString(sessionObj.events()),
        sessionObj.lastUpdateTime());
  }

  public Optional<Session> getSession(String appName, String userId, String sessionId)
      throws JsonProcessingException {
    String selectCql =
        "SELECT app_name, user_id, session_id, state, events, last_update_time FROM sessions WHERE app_name = ? AND user_id = ? AND session_id = ?";
    Row row = session.execute(selectCql, appName, userId, sessionId).one();

    if (row == null) {
      return Optional.empty();
    }

    String storedAppName = row.getString("app_name");
    String storedUserId = row.getString("user_id");
    String storedSessionId = row.getString("session_id");
    String stateJson = row.getString("state");
    String eventsJson = row.getString("events");
    Instant lastUpdateTime = row.getInstant("last_update_time");

    ConcurrentMap<String, Object> state =
        objectMapper.readValue(
            stateJson,
            objectMapper
                .getTypeFactory()
                .constructMapType(ConcurrentMap.class, String.class, Object.class));
    List<Event> events =
        objectMapper.readValue(
            eventsJson,
            objectMapper.getTypeFactory().constructCollectionType(List.class, Event.class));

    Session retrievedSession =
        Session.builder(storedSessionId)
            .appName(storedAppName)
            .userId(storedUserId)
            .state(state)
            .events(events)
            .lastUpdateTime(lastUpdateTime)
            .build();

    return Optional.of(retrievedSession);
  }

  public List<Session> listSessions(String appName, String userId) {
    String selectCql =
        "SELECT app_name, user_id, session_id, last_update_time FROM sessions WHERE app_name = ? AND user_id = ?";
    ResultSet resultSet = session.execute(selectCql, appName, userId);
    List<Session> sessionCopies = new ArrayList<>();
    for (Row row : resultSet) {
      String storedAppName = row.getString("app_name");
      String storedUserId = row.getString("user_id");
      String storedSessionId = row.getString("session_id");
      Instant lastUpdateTime = row.getInstant("last_update_time");

      sessionCopies.add(
          Session.builder(storedSessionId)
              .appName(storedAppName)
              .userId(storedUserId)
              .lastUpdateTime(lastUpdateTime)
              .build());
    }
    return sessionCopies;
  }

  public void deleteSession(String appName, String userId, String sessionId) {
    String deleteCql = "DELETE FROM sessions WHERE app_name = ? AND user_id = ? AND session_id = ?";
    session.execute(deleteCql, appName, userId, sessionId);
  }

  public List<Event> listEvents(String appName, String userId, String sessionId)
      throws JsonProcessingException {
    String selectCql =
        "SELECT events FROM sessions WHERE app_name = ? AND user_id = ? AND session_id = ?";
    Row row = session.execute(selectCql, appName, userId, sessionId).one();
    if (row == null) {
      return ImmutableList.of();
    }
    String eventsJson = row.getString("events");
    return objectMapper.readValue(
        eventsJson, objectMapper.getTypeFactory().constructCollectionType(List.class, Event.class));
  }

  public void updateSessionEvents(
      String appName, String userId, String sessionId, List<Event> events, Instant lastUpdateTime)
      throws JsonProcessingException {
    String updateCql =
        "UPDATE sessions SET events = ?, last_update_time = ? WHERE app_name = ? AND user_id = ? AND session_id = ?";
    session.execute(
        updateCql,
        objectMapper.writeValueAsString(events),
        lastUpdateTime,
        appName,
        userId,
        sessionId);
  }

  // --- Artifact Service Methods ---

  private static final String ARTIFACT_TABLE = "artifacts";
  private static final String FILENAMES_TABLE = "session_filenames_by_user";

  public void deleteArtifact(String appName, String userId, String sessionId, String filename) {
    String deleteCql =
        "DELETE FROM "
            + ARTIFACT_TABLE
            + " WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?";
    session.execute(deleteCql, appName, userId, sessionId, filename);
  }

  public List<String> listArtifactKeys(String appName, String userId, String sessionId) {
    String selectCql =
        "SELECT filename FROM "
            + FILENAMES_TABLE
            + " WHERE app_name = ? AND user_id = ? AND session_id = ?";
    ResultSet resultSet = session.execute(selectCql, appName, userId, sessionId);
    List<String> filenames = new ArrayList<>();
    for (Row row : resultSet) {
      filenames.add(row.getString("filename"));
    }
    return filenames;
  }

  public List<Integer> listArtifactVersions(
      String appName, String userId, String sessionId, String filename) {
    String selectCql =
        "SELECT version_number FROM "
            + ARTIFACT_TABLE
            + " WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?";
    ResultSet resultSet = session.execute(selectCql, appName, userId, sessionId, filename);
    List<Integer> versions = new ArrayList<>();
    for (Row row : resultSet) {
      versions.add(row.getInt("version_number"));
    }
    return versions;
  }

  public Optional<Part> loadArtifact(
      String appName, String userId, String sessionId, String filename, Optional<Integer> version)
      throws JsonProcessingException {
    String selectCql;
    Row row;
    if (version.isPresent()) {
      selectCql =
          "SELECT artifact_data FROM "
              + ARTIFACT_TABLE
              + " WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? AND version_number = ?";
      row = session.execute(selectCql, appName, userId, sessionId, filename, version.get()).one();
    } else {
      selectCql =
          "SELECT artifact_data FROM "
              + ARTIFACT_TABLE
              + " WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ? ORDER BY version_number DESC LIMIT 1";
      row = session.execute(selectCql, appName, userId, sessionId, filename).one();
    }

    if (row == null) {
      return Optional.empty();
    }
    String artifactJson = row.getString("artifact_data");

    // Convert JSON string to Map<String, Object>
    Map<String, Object> artifactMap =
        objectMapper.readValue(artifactJson, new TypeReference<Map<String, Object>>() {});

    // Manually construct Part from the map
    Part.Builder builder = Part.builder();

    if (artifactMap.containsKey("text") && artifactMap.get("text") != null) {
      builder.text((String) artifactMap.get("text"));
    }
    if (artifactMap.containsKey("inlineData") && artifactMap.get("inlineData") != null) {
      // Need to convert map to Blob object
      builder.inlineData(objectMapper.convertValue(artifactMap.get("inlineData"), Blob.class));
    }
    if (artifactMap.containsKey("fileData") && artifactMap.get("fileData") != null) {
      // Need to convert map to FileData object
      builder.fileData(objectMapper.convertValue(artifactMap.get("fileData"), FileData.class));
    }
    if (artifactMap.containsKey("functionCall") && artifactMap.get("functionCall") != null) {
      // Need to convert map to FunctionCall object
      builder.functionCall(
          objectMapper.convertValue(artifactMap.get("functionCall"), FunctionCall.class));
    }
    if (artifactMap.containsKey("functionResponse")
        && artifactMap.get("functionResponse") != null) {
      // Need to convert map to FunctionResponse object
      builder.functionResponse(
          objectMapper.convertValue(artifactMap.get("functionResponse"), FunctionResponse.class));
    }
    // The 'thought' field is a boolean Optional<Boolean>
    if (artifactMap.containsKey("thought") && artifactMap.get("thought") != null) {
      builder.thought((Boolean) artifactMap.get("thought"));
    }

    return Optional.of(builder.build());
  }

  public int saveArtifact(
      String appName, String userId, String sessionId, String filename, Part artifact)
      throws JsonProcessingException {
    String selectMaxVersionCql =
        "SELECT MAX(version_number) FROM "
            + ARTIFACT_TABLE
            + " WHERE app_name = ? AND user_id = ? AND session_id = ? AND filename = ?";
    int nextVersion = 0;
    Row row = session.execute(selectMaxVersionCql, appName, userId, sessionId, filename).one();
    if (row != null && !row.isNull(0)) {
      nextVersion = row.getInt(0) + 1;
    }

    // Convert Part to Map<String, Object> to avoid direct Part serialization issues
    Map<String, Object> artifactMap =
        objectMapper.convertValue(artifact, new TypeReference<Map<String, Object>>() {});

    String insertCql =
        "INSERT INTO "
            + ARTIFACT_TABLE
            + " (app_name, user_id, session_id, filename, version_number, artifact_data) VALUES (?, ?, ?, ?, ?, ?)";
    String insertFilenamesCql =
        "INSERT INTO "
            + FILENAMES_TABLE
            + " (app_name, user_id, session_id, filename) VALUES (?, ?, ?, ?)";

    session.execute(
        insertCql,
        appName,
        userId,
        sessionId,
        filename,
        nextVersion,
        objectMapper.writeValueAsString(artifactMap)); // Save the map as JSON string
    session.execute(insertFilenamesCql, appName, userId, sessionId, filename);
    return nextVersion;
  }

  public void close() {
    if (session != null) {
      session.close();
    }
  }

  public List<InetSocketAddress> contactPointsParser(String contactPoints) {
    List<InetSocketAddress> addresses = new ArrayList<>();
    if (contactPoints == null || contactPoints.trim().isEmpty()) {
      throw new IllegalArgumentException("Contact points are required");
    }
    String[] parts = contactPoints.split(",");
    for (String part : parts) {
      String[] hostPort = part.split(":");
      if (hostPort.length == 2) {
        try {
          addresses.add(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid port: " + hostPort[1], e);
        }
      } else {
        try {
          addresses.add(new InetSocketAddress(hostPort[0], Integer.parseInt(DEFAULT_PORT)));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid port: " + DEFAULT_PORT, e);
        }
      }
    }
    return addresses;
  }
}
