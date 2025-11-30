package pool;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

public class ConnectionPool {

    private static final MongoClient CLIENT;
    private static final MongoDatabase DB;

    static {
        ResourceBundle config = ResourceBundle.getBundle("config.classConfig");
        String uri = config.getString("Conn");
        String dbName = config.getString("Database");

        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .applyToConnectionPoolSettings(builder ->
                builder
                    .maxSize(Integer.parseInt(config.getString("maxPoolSize")))
                    .minSize(Integer.parseInt(config.getString("minPoolSize")))
                    .maxWaitTime(Long.parseLong(config.getString("maxWaitMillis")), TimeUnit.MILLISECONDS)
            )
            .build();

        CLIENT = MongoClients.create(settings);
        DB = CLIENT.getDatabase(dbName);
    }

    public static MongoDatabase getDatabase() {
        return DB;
    }
    
    public static void close() {
        if (CLIENT != null) {
            CLIENT.close();
        }
    }
}
