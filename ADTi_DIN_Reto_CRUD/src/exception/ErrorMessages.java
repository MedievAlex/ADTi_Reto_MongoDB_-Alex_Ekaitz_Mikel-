package exception;

/**
 * Utility class containing predefined error messages for the application. This class serves as a centralized repository for all user-facing and system error messages, ensuring consistency in error reporting throughout the application.
 *
 * All messages are defined as public static final constants to facilitate easy maintenance and internationalization of error texts.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class ErrorMessages
{

    /**
     * Error message displayed when user registration fails. This typically occurs due to database constraints, connection issues, or system errors during the registration process.
     */
    public static final String REGISTER_USER = "User could not be registered. Please try again later.";

    /**
     * Error message displayed when retrieving the list of users fails. This typically occurs due to database connection issues, query errors, or data access problems.
     */
    public static final String GET_USERS = "Failed to retrieve users.";

    /**
     * Error message displayed when user profile update fails. This typically occurs due to database constraints, validation errors, or system errors during the update process.
     */
    public static final String UPDATE_USER = "User could not be updated.";

    /**
     * Error message displayed when user deletion fails. This typically occurs due to database constraints, foreign key violations, or system errors during the deletion process.
     */
    public static final String DELETE_USER = "User could not be deleted.";

    /**
     * Error message displayed when user authentication fails. This typically occurs due to invalid credentials, user not found, or system errors during the login process.
     */
    public static final String LOGIN = "Login failed. Please check your credentials.";

    /**
     * Error message displayed when credential verification fails. This typically occurs during registration when checking for duplicate emails or usernames, and the verification process encounters errors.
     */
    public static final String VERIFY_CREDENTIALS = "Could not verify existing credentials.";

    /**
     * Error message displayed when a database transaction rollback fails. This typically occurs during atomic operations when one part fails and the system cannot properly revert the transaction.
     */
    public static final String ROLLBACK = "Transaction rollback failed.";

    /**
     * Error message displayed when resetting database connection auto-commit fails. This typically occurs after transaction operations when the system cannot restore the connection to its default auto-commit state.
     */
    public static final String RESET_AUTOCOMMIT = "Failed to reset connection settings.";

    /**
     * Error message displayed when database connection timeout occurs. This typically occurs when the connection pool cannot provide a connection within the expected time frame due to high load or resource constraints.
     */
    public static final String TIMEOUT = "Timeout retrieving a connection.";

    /**
     * Error message displayed when database initialization fails. This typically occurs during application startup when the system cannot establish initial connection to the database or initialize the data source.
     */
    public static final String DATABASE = "Error initializing database connection.";
}
