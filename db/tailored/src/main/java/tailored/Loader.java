package tailored;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Loader {
  public static void runLoad(Dbms dbms, Path nodesPath, Path edgesPath) {
    BenchmarkConfig config = BenchmarkConfig.create(
        dbms,
        WorkloadType.FOF, // dummy, not used during load
        1,
        0,
        0,
        null
    );

    try (BenchmarkContext ctx = ConnectionFactory.openContext(config)) {
      switch (dbms) {
      case POSTGRES -> loadPostgres(ctx.pgConn, nodesPath, edgesPath);
      case NEO4J -> loadNeo4j(ctx.neoDriver, nodesPath, edgesPath);
      default -> throw new IllegalArgumentException("Unsupported DBMS: " + dbms);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load data", e);
    }
  }

  private static void loadPostgres(Connection conn, Path nodesPath, Path edgesPath) throws SQLException {
    if (conn == null) {
      throw new IllegalStateException("Postgres connection is null in BenchmarkContext");
    }

    conn.setAutoCommit(false);

    try (Statement st = conn.createStatement()) {
      st.executeUpdate("TRUNCATE TABLE edges");
      st.executeUpdate("TRUNCATE TABLE nodes");
    }

    String nodeSql = """
        INSERT INTO nodes (
          id,
          public,
          completion_pct,
          gender,
          region,
          last_login,
          registration,
          age,
          education,
          smoking,
          alcohol
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
        """;

    try (PreparedStatement ps = conn.prepareStatement(nodeSql); Stream<String> lines = Files.lines(nodesPath)) {
      final int batchSize = 10_000;
      int batchCount = 0;

      for (String line : (Iterable<String>) lines::iterator) {
        String[] cols = line.split("\t", -1);
        if (cols.length < 8) {
          continue; // corrupt line, ignore
        }

        long userId = Long.parseLong(cols[0]);
        Integer pub = parseIntOrNull(cols[1]);
        Integer completion = parseIntOrNull(cols[2]);
        Integer gender = parseIntOrNull(cols[3]);
        String region = parseTextOrNull(cols[4]);
        String lastLogin = parseTextOrNull(cols[5]);
        String registration = parseTextOrNull(cols[6]);
        Integer age = parseIntOrNull(cols[7]);
        String education = cols.length > 19 ? parseTextOrNull(cols[19]) : null;
        String smoking = cols.length > 22 ? parseTextOrNull(cols[22]) : null;
        String alcohol = cols.length > 23 ? parseTextOrNull(cols[23]) : null;

        int i = 1;
        ps.setLong(i++, userId);
        if (pub != null) {
          ps.setInt(i++, pub);
        } else {
          ps.setNull(i++, Types.INTEGER);
        }
        if (completion != null) {
          ps.setInt(i++, completion);
        } else {
          ps.setNull(i++, Types.INTEGER);
        }
        if (gender != null) {
          ps.setInt(i++, gender);
        } else {
          ps.setNull(i++, Types.INTEGER);
        }
        ps.setString(i++, region);
        ps.setString(i++, lastLogin);
        ps.setString(i++, registration);
        if (age != null) {
          ps.setInt(i++, age);
        } else {
          ps.setNull(i++, Types.INTEGER);
        }
        ps.setString(i++, education);
        ps.setString(i++, smoking);
        ps.setString(i++, alcohol);

        ps.addBatch();
        if (++batchCount % batchSize == 0) {
          ps.executeBatch();
        }
      }
      ps.executeBatch();
      String edgeSql = "INSERT INTO edges (start_id, end_id) VALUES (?, ?)";
      try (PreparedStatement ps2 = conn.prepareStatement(edgeSql);
           Stream<String> lines2 = Files.lines(edgesPath)) {

        final int batchSize2 = 20_000;
        int batchCount2 = 0;

        for (String line : (Iterable<String>) lines2::iterator) {
          String[] cols = line.split("\t", -1);
          if (cols.length < 2) {
            continue;
          }

          long u = Long.parseLong(cols[0]);
          long v = Long.parseLong(cols[1]);

          ps2.setLong(1, u);
          ps2.setLong(2, v);
          ps2.addBatch();

          if (++batchCount2 % batchSize2 == 0) {
            ps2.executeBatch();
          }
        }
        ps2.executeBatch();
      }

      conn.commit();
      conn.setAutoCommit(true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void loadNeo4j(Driver driver, Path nodesPath, Path edgesPath) throws IOException {
    if (driver == null) {
      throw new IllegalStateException("Neo4j driver is null in BenchmarkContext");
    }

    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
      // wipe graph
      session.executeWrite(tx -> {
        tx.run("MATCH (n) DETACH DELETE n");
        return null;
      });

      final int batchSize = 10_000;
      try (Stream<String> lines = Files.lines(nodesPath)) {
        List<Map<String, Object>> batch = new ArrayList<>(batchSize);

        for (String line : (Iterable<String>) lines::iterator) {
          String[] cols = line.split("\t", -1);
          if (cols.length < 8) {
            continue;
          }

          long userId = Long.parseLong(cols[0]);
          Integer pub = parseIntOrNull(cols[1]);
          Integer completion = parseIntOrNull(cols[2]);
          Integer gender = parseIntOrNull(cols[3]);
          String region = parseTextOrNull(cols[4]);
          String lastLogin = parseTextOrNull(cols[5]);
          String registration = parseTextOrNull(cols[6]);
          Integer age = parseIntOrNull(cols[7]);
          String education = cols.length > 19 ? parseTextOrNull(cols[19]) : null;
          String smoking = cols.length > 22 ? parseTextOrNull(cols[22]) : null;
          String alcohol = cols.length > 23 ? parseTextOrNull(cols[23]) : null;

          Map<String, Object> m = new HashMap<>();
          m.put("id", userId);
          if (pub != null) {
            m.put("public", pub);
          }
          if (completion != null) {
            m.put("completion_pct", completion);
          }
          if (gender != null) {
            m.put("gender", gender);
          }
          if (region != null) {
            m.put("region", region);
          }
          if (lastLogin != null) {
            m.put("last_login", lastLogin);
          }
          if (registration != null) {
            m.put("registration", registration);
          }
          if (age != null) {
            m.put("age", age);
          }
          if (education != null) {
            m.put("education", education);
          }
          if (smoking != null) {
            m.put("smoking", smoking);
          }
          if (alcohol != null) {
            m.put("alcohol", alcohol);
          }

          batch.add(m);

          if (batch.size() >= batchSize) {
            flushNeoNodes(session, batch);
            batch.clear();
          }
        }
        if (!batch.isEmpty()) {
          flushNeoNodes(session, batch);
        }
      }

      final int edgeBatchSize = 10_000;
      try (Stream<String> lines = Files.lines(edgesPath)) {
        List<Map<String, Object>> batch = new ArrayList<>(edgeBatchSize);

        for (String line : (Iterable<String>) lines::iterator) {
          String[] cols = line.split("\t", -1);
          if (cols.length < 2) {
            continue;
          }

          long u = Long.parseLong(cols[0]);
          long v = Long.parseLong(cols[1]);

          batch.add(Map.of("start", u, "end", v));

          if (batch.size() >= edgeBatchSize) {
            flushNeoEdges(session, batch);
            batch.clear();
          }
        }
        if (!batch.isEmpty()) {
          flushNeoEdges(session, batch);
        }
      }
    }
  }

  private static void flushNeoNodes(Session session, List<Map<String, Object>> batch) {
    session.executeWrite(tx -> {
      tx.run("""
        UNWIND $nodes AS n
        MERGE (p:Person {id: n.id})
        SET
        p.public          = n.public,
        p.completion_pct  = n.completion_pct,
        p.gender          = n.gender,
        p.region          = n.region,
        p.last_login      = n.last_login,
        p.registration    = n.registration,
        p.age             = n.age,
        p.education       = n.education,
        p.smoking         = n.smoking,
        p.alcohol         = n.alcohol
      """, Map.of("nodes", batch));
      return null;
    });
  }

  private static void flushNeoEdges(Session session, List<Map<String, Object>> batch) {
    session.executeWrite(tx -> {
      tx.run("""
          UNWIND $edges AS e
          MATCH (u:Person {id: e.start}), (v:Person {id: e.end})
          MERGE (u)-[:FRIENDS_WITH]->(v)
          """, Map.of("edges", batch));
      return null;
    });
  }

  private static Integer parseIntOrNull(String s) {
    if (s == null || s.isEmpty() || s.equals("null")) {
      return null;
    }
    return Integer.parseInt(s);
  }

  private static String parseTextOrNull(String s) {
    if (s == null || s.isEmpty() || s.equals("null")) {
      return null;
    }
    return s;
  }
}
