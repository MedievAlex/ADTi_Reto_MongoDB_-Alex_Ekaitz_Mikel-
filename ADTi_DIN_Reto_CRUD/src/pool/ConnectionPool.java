package pool;

import org.apache.commons.dbcp2.BasicDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ResourceBundle;

/**
 * Database connection pool implementation using Apache Commons DBCP2. This class provides a singleton connection pool that manages database connections efficiently by reusing existing connections rather than creating new ones for each request.
 *
 * The pool is configured with optimal parameters for connection management, including initial size, maximum connections, and timeout settings to ensure reliable database access while preventing resource exhaustion.
 */
public class ConnectionPool
{

    // Pool unique instance
    private static final BasicDataSource DATASOURCE;

    // Pool static configuration
    static
    {
        DATASOURCE = new BasicDataSource();

        // Import configuration from classConfig.properties file
        ResourceBundle configFile = ResourceBundle.getBundle("config.classConfig");

        DATASOURCE.setUrl(configFile.getString("Conn"));
        DATASOURCE.setUsername(configFile.getString("DBUser"));
        DATASOURCE.setPassword(configFile.getString("DBPass"));
        DATASOURCE.setDriverClassName(configFile.getString("Driver"));

        // Pool parameters
        DATASOURCE.setInitialSize(2);      // Start connections
        DATASOURCE.setMaxTotal(4);        // Max total connextions
        DATASOURCE.setMinIdle(2);          // Min inactive connections
        DATASOURCE.setMaxIdle(4);          // Max inactive connections
        DATASOURCE.setMaxWaitMillis(10000); // Max wait time for a connection
    }

    /**
     * Retrieves a database connection from the connection pool. This method obtains a connection from the pool, which may be a newly created connection or a reused existing connection. The connection should be closed after use to return it to the pool for reuse.
     *
     * @return a Connection object that can be used to execute SQL statements
     * @throws SQLException if a database access error occurs or the maximum wait time for a connection is exceeded
     */
    public static Connection getConnection() throws SQLException
    {
        return DATASOURCE.getConnection();
    }
}
