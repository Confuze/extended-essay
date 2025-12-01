// load phase based on file paths of data specified as args

package tailored;

import org.neo4j.driver.Driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
                case NEO4J    -> loadNeo4j(ctx.neoDriver, nodesPath, edgesPath);
                default       -> throw new IllegalArgumentException("Unsupported DBMS: " + dbms);
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

                
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadNeo4j(Driver driver, Path nodesPath, Path edgesPath) {
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null || s.isEmpty() || s.equals("null")) return null;
        return Integer.parseInt(s);
    }

    private static String parseTextOrNull(String s) {
        if (s == null || s.isEmpty() || s.equals("null")) return null;
        return s;
    }
}
