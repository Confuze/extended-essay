// exposes methods for connecting with dbmss and generating random candidate ids

package tailored;

import org.neo4j.driver.*;

import java.sql.Connection;
import java.sql.DriverManager;

public final class ConnectionFactory {
    private ConnectionFactory() {}

    public static BenchmarkContext openContext(BenchmarkConfig config) throws Exception {
        return switch (config.dbms()) {
            case POSTGRES -> openPostgresContext(config);
            case NEO4J -> openNeo4jContext(config);
        };
    }

    private static BenchmarkContext openPostgresContext(BenchmarkConfig config) throws Exception {
        if (config.dbms() != Dbms.POSTGRES) throw new IllegalStateException();

        assert config.pgUrl() != null;
        Connection conn = DriverManager.getConnection(config.pgUrl(), config.pgUser(), config.pgPassword());

        return new BenchmarkContext(config, conn, null);
    }

    private static BenchmarkContext openNeo4jContext(BenchmarkConfig config) throws Exception {
        if (config.dbms() != Dbms.NEO4J) throw new IllegalStateException();

        Driver driver;

        try {
            assert config.neo4jUser() != null;
            assert config.neo4jPassword() != null;
            driver = GraphDatabase.driver(config.neo4jUri(), AuthTokens.basic(config.neo4jUser(), config.neo4jPassword()));
            driver.verifyConnectivity();
            System.out.println("Neo4j client connection created with " + config.neo4jUri());


        } catch (Exception e) {
            throw new Exception("Could not initialize connection to neo4j: " + e.toString(), e);
        }

        return new BenchmarkContext(config, null, driver);
    }
}