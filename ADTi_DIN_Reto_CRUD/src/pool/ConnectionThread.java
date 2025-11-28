package pool;

import exception.OurException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Thread class for managing database connections with timeout and delayed release functionality. This class extends Thread to handle database connection operations asynchronously, providing connection pooling with controlled lifecycle management and error handling.
 *
 * The thread obtains a connection from the pool, maintains it for a specified duration, and automatically releases it after use with configurable delay for connection reuse optimization.
 */
public class ConnectionThread extends Thread
{

    private int delay = 30;
    private boolean end = false;
    private boolean ready = false;
    private Connection con;
    private OurException exception;

    /**
     * Constructs a new ConnectionThread with the specified delay for connection release.
     *
     * @param delay the number of seconds to wait before releasing the connection after it's no longer needed, allowing for connection reuse
     */
    public ConnectionThread(int delay)
    {
        this.delay = delay;
    }

    /**
     * Retrieves the database connection managed by this thread. This method returns the connection once it has been successfully obtained from the connection pool and the thread is ready.
     *
     * @return the Connection object for database operations
     * @throws OurException if an error occurred while obtaining the connection from the pool during thread execution
     */
    public Connection getConnection() throws OurException
    {
        if (exception != null)
        {
            throw exception;
        }

        return con;
    }

    /**
     * Checks if the connection thread is ready to provide a connection. This method indicates whether the thread has successfully obtained a database connection from the pool and is prepared for database operations.
     *
     * @return true if the connection is ready for use, false otherwise
     */
    public boolean isReady()
    {
        return ready;
    }

    /**
     * Signals the thread to release the connection and terminate. This method interrupts the thread's waiting state and initiates the connection release process with the configured delay.
     */
    public void releaseConnection()
    {
        this.end = true;
        this.interrupt();
    }

    /**
     * The main execution method of the connection thread. This method obtains a connection from the pool, maintains it until released, and then waits for the specified delay before closing the connection. It handles SQL exceptions and converts them to application-specific exceptions.
     */
    @Override
    public void run()
    {
        try
        {
            con = ConnectionPool.getConnection();
            ready = true;

            while (!end)
            {
                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException e)
                {
                    if (end)
                    {
                        break;
                    }

                    Thread.currentThread().interrupt();
                }
            }

            try
            {
                Thread.sleep(delay * 1000);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        catch (SQLException ex)
        {
            exception = new OurException("Error obtaining a connection from pool: " + ex.getMessage());
        } finally
        {
            try
            {
                if (con != null)
                {
                    con.close();
                }
            }
            catch (SQLException ex)
            {
                exception = new OurException("Error returning the connection: " + ex.getMessage());
            }
        }
    }
}
