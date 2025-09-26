package com.google.adk.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.sessions.Session;
import java.sql.*;
import java.time.Instant;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Arun Parmar
 */
public class PostgresDBHelper {
  private static final Logger logger = LoggerFactory.getLogger(PostgresDBHelper.class);
  private static volatile PostgresDBHelper instance;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final String DB_URL = "DBURL";
  private static final String USER = "DBUSER";
  private static final String PASSWORD = "DBPASSWORD";
  private static final String SESSIONS_TABLE = "sessions";

  private PostgresDBHelper() {
    // Initialize any non-connection resources here
    // Don't create database connections in constructor
    logger.info("PostgresDBHelper instance created.");
  }

  // Thread-safe singleton using double-checked locking
  public static PostgresDBHelper getInstance() {
    if (instance == null) {
      synchronized (PostgresDBHelper.class) {
        if (instance == null) {
          instance = new PostgresDBHelper();
        }
      }
    }
    return instance;
  }

  // Create a new connection for each operation - thread-safe approach
  private Connection getConnection() throws SQLException {
    DriverManager.setLoginTimeout(10); // timeout in seconds
    String userName = System.getenv(USER);
    String password = System.getenv(PASSWORD);
    String host = System.getenv(DB_URL);

    Connection conn = DriverManager.getConnection(host, userName, password);
    if (conn == null) {
      throw new SQLException("Failed to establish a connection to the database.");
    }
    return conn;
  }

  /**
   * Saves a session to the PostgreSQL database. This method acts as an upsert (INSERT if not
   * exists, UPDATE if exists). It parses the incoming session JSON string to extract fields and
   * handle events.
   *
   * @param sessionId The ID of the session.
   * @param session object.
   * @throws SQLException If a database access error occurs.
   * @throws JsonProcessingException If there's an error processing JSON.
   */
  public void saveSession(String sessionId, Session session)
      throws SQLException, JsonProcessingException {
    // Use a transaction for atomicity
    try (Connection conn = this.getConnection()) {
      conn.setAutoCommit(false); // Start transaction

      // Upsert the main session data
      String upsertSql =
          "INSERT INTO "
              + SESSIONS_TABLE
              + " (id, app_name, user_id, state, last_update_time, event_data) VALUES (?, ?, ?, CAST(? AS JSONB), ?, CAST(? AS JSONB)) "
              + "ON CONFLICT (id) DO UPDATE SET app_name = EXCLUDED.app_name, user_id = EXCLUDED.user_id, state = EXCLUDED.state, last_update_time = EXCLUDED.last_update_time, event_data = EXCLUDED.event_data";

      try (PreparedStatement pstmt = conn.prepareStatement(upsertSql)) {
        JSONObject eventJson = new JSONObject();
        eventJson.put("events", session.events().toString());
        // System.out.println("Event JSON: " + eventJson.toString());
        pstmt.setString(1, session.id());
        pstmt.setString(2, session.appName());
        pstmt.setString(3, session.userId());
        pstmt.setString(4, objectMapper.writeValueAsString(session.state()));
        pstmt.setObject(5, Timestamp.from(session.lastUpdateTime()));
        pstmt.setString(6, eventJson.toString());
        /** Execute the upsert statement. */
        pstmt.executeUpdate();

        insertEvents(conn, eventJson, session);

        logger.debug("Session {} saved/updated successfully.", sessionId);
      }
      /**
       * Commits the transaction happening inside the insertEvents function so no need here.
       * conn.commit();
       */
      logger.debug("Session {} saved/updated successfully.", sessionId);
    } catch (SQLException e) {
      e.printStackTrace();
      logger.error("Error saving session {}. Rolling back transaction.", sessionId, e);
      throw e;
    }
  }

  private void insertEvents(Connection conn, JSONObject eventJson, Session session) {
    try {
      /*
       * This method inserts events into the database.
       * It expects the eventJson to contain an "events" array.
       * It parses the events and inserts them into the database.
       * Though the event is already a valid JSON, in case eventJson is a JsonObject
       * as a string, then we need to take care of this.
       */
      String raw = eventJson.get("events").toString();
      if (raw.startsWith("\"")) {
        raw = raw.substring(1, raw.length() - 1).replace("\\\"", "\"");
      }

      JSONObject corrected = new JSONObject("{" + "\"events\":" + raw + "}");
      JSONArray events = corrected.getJSONArray("events");

      conn.setAutoCommit(false);
      try (PreparedStatement stmtEvents =
              conn.prepareStatement(
                  "INSERT INTO events (id, session_id, author, actions_state_delta, actions_artifact_delta, "
                      + "actions_requested_auth_configs, actions_transfer_to_agent, content_role, timestamp, invocation_id) "
                      + "VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET "
                      + "session_id=EXCLUDED.session_id, author=EXCLUDED.author, actions_state_delta=EXCLUDED.actions_state_delta, "
                      + "actions_artifact_delta=EXCLUDED.actions_artifact_delta, actions_requested_auth_configs=EXCLUDED.actions_requested_auth_configs, "
                      + "actions_transfer_to_agent=EXCLUDED.actions_transfer_to_agent, content_role=EXCLUDED.content_role, "
                      + "timestamp=EXCLUDED.timestamp, invocation_id=EXCLUDED.invocation_id");
          PreparedStatement stmtParts =
              conn.prepareStatement(
                  "INSERT INTO event_content_parts (event_id, session_id, part_type, text_content, function_call_id, "
                      + "function_call_name, function_call_args, function_response_id, function_response_name, function_response_data) "
                      + "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?::jsonb) ON CONFLICT (event_id) DO UPDATE SET "
                      + "session_id=EXCLUDED.session_id, part_type=EXCLUDED.part_type, text_content=EXCLUDED.text_content, "
                      + "function_call_id=EXCLUDED.function_call_id, function_call_name=EXCLUDED.function_call_name, "
                      + "function_call_args=EXCLUDED.function_call_args, function_response_id=EXCLUDED.function_response_id, "
                      + "function_response_name=EXCLUDED.function_response_name, function_response_data=EXCLUDED.function_response_data")) {
        for (int i = 0; i < events.length(); i++) {
          JSONObject ev = events.getJSONObject(i);
          //          logger.info("Processing event for insertion: {}", ev.toString());
          JSONObject actions = ev.getJSONObject("actions");
          JSONObject content = ev.optJSONObject("content");
          if (content == null) {
            content = new JSONObject();
          }
          JSONArray parts = content.optJSONArray("parts");
          if (parts == null) {
            parts = new JSONArray();
          }

          stmtEvents.setString(1, ev.getString("id"));
          stmtEvents.setString(2, session.id());
          stmtEvents.setString(3, ev.optString("author"));
          stmtEvents.setString(4, actions.opt("stateDelta").toString());
          stmtEvents.setString(5, actions.opt("artifactDelta").toString());
          stmtEvents.setString(6, actions.opt("requestedAuthConfigs").toString());
          stmtEvents.setString(7, actions.optString("transferToAgent", null));
          stmtEvents.setString(8, content.optString("role"));
          stmtEvents.setLong(9, ev.getLong("timestamp"));
          stmtEvents.setString(10, ev.optString("invocationId"));
          // System.out.println("Executing event insert: " + stmtEvents.toString());
          stmtEvents.executeUpdate();
          conn.commit();

          for (int j = 0; j < parts.length(); j++) {
            JSONObject part = parts.getJSONObject(j);
            String type =
                part.has("text")
                    ? "text"
                    : part.has("functionCall") ? "functionCall" : "functionResponse";

            stmtParts.setString(1, ev.getString("id"));
            stmtParts.setString(2, session.id());
            stmtParts.setString(3, type);
            stmtParts.setString(4, part.optString("text", null));

            JSONObject fc = part.optJSONObject("functionCall");
            JSONObject fr = part.optJSONObject("functionResponse");

            stmtParts.setString(5, fc != null ? fc.optString("id", null) : null);
            stmtParts.setString(6, fc != null ? fc.optString("name", null) : null);
            stmtParts.setString(7, fc != null ? fc.opt("args").toString() : null);

            stmtParts.setString(8, fr != null ? fr.optString("id", null) : null);
            stmtParts.setString(9, fr != null ? fr.optString("name", null) : null);
            stmtParts.setString(10, fr != null ? fr.opt("response").toString() : null);
            // System.out.println("Executing event insert: " + stmtParts.toString());

            stmtParts.executeUpdate();
          }
        }
        conn.commit();
      } catch (Exception ex) {
        logger.error(
            "Error inserting events for session {}. Rolling back transaction.", session.id(), ex);
        // Rollback the transaction in case of error
        if (conn != null && !conn.isClosed()) {
          conn.rollback();
        }
        // throw ex;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Retrieves a session from the PostgreSQL database by ID. Reconstructs the JSONObject to match
   * the format expected by
   *
   * @param id The ID of the session.
   * @return A JSONObject representing the session, or null if not found.
   * @throws SQLException If a database access error occurs.
   */
  public JSONObject getSession(String id) throws SQLException {
    String sql =
        "SELECT id, app_name, user_id, state, last_update_time,event_data FROM "
            + SESSIONS_TABLE
            + " WHERE id = ?";
    JSONObject sessionJson = null;

    try (Connection conn = PostgresDBHelper.getInstance().getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {

      pstmt.setString(1, id);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          // Extract data from ResultSet
          String sessionId = rs.getString("id");
          String appName = rs.getString("app_name");
          String userId = rs.getString("user_id");
          String stateDataJson = rs.getString("state"); // This is the JSONB string
          Instant lastUpdateTime = rs.getTimestamp("last_update_time").toInstant();
          JSONObject eventJson = new JSONObject(rs.getString("event_data"));
          JSONArray eventArr = new JSONArray(eventJson.getString("events"));

          // Create a Jackson JsonNode to reconstruct the Session structure expected by
          sessionJson = new JSONObject();
          sessionJson.put("id", sessionId);
          sessionJson.put("appName", appName);
          sessionJson.put("userId", userId);
          sessionJson.put("state", new JSONObject(stateDataJson));
          sessionJson.put("events", eventArr);
          sessionJson.put(
              "lastUpdateTime",
              (double) lastUpdateTime.getEpochSecond()
                  + lastUpdateTime.getNano() / 1_000_000_000.0);

          logger.debug("Session {} retrieved successfully.", sessionId);
        } else {
          logger.warn("Session with ID {} not found.", id);
          return null; // Return null if session not found
        }
      }
    }
    return sessionJson;
  }

  /**
   * Deletes a session and its associated events from the database.
   *
   * @param sessionId The ID of the session to delete.
   * @throws SQLException If a database access error occurs.
   */
  public void deleteSession(String sessionId) throws SQLException {
    // Due to ON DELETE CASCADE, deleting from 'sessions' table will automatically
    // delete associated events in the 'events' table.
    String sql = "DELETE FROM " + SESSIONS_TABLE + " WHERE id = ?";
    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, sessionId);
      int rowsAffected = pstmt.executeUpdate();
      if (rowsAffected > 0) {
        logger.info("Session {} and its events deleted successfully.", sessionId);
      } else {
        logger.warn("Attempted to delete session {}, but it was not found.", sessionId);
      }
    }
  }
}
