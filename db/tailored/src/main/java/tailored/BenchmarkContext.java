// record with entire benchmark context: config + connection + generated random ids

package tailored;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import reactor.util.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

public final class BenchmarkContext implements AutoCloseable {
    public final BenchmarkConfig config;
    @Nullable public final Connection pgConn;
    @Nullable public final Driver neoDriver;
    @Nullable public final Session neoSession;
    public final ThreadLocal<Random> random;

    public BenchmarkContext(BenchmarkConfig config, Connection pgConn, Driver neoDriver, Session neoSession) {
        this.config = config;
        this.pgConn = pgConn;
        this.neoDriver = neoDriver;
        this.neoSession = neoSession;
        this.random = ThreadLocal.withInitial(() -> new Random(123));
    }

    @Override
    public void close() throws Exception {
        if (pgConn != null)
            try {
                pgConn.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close PostgreSQL connection", e);
            }
        else if (neoDriver != null)
            neoDriver.close();
    }
}
