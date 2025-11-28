package exception;

/**
 * Custom exception class for handling application-specific errors. This exception is used throughout the application to represent business logic errors, data access issues, and other application-specific failure conditions.
 *
 * Extends the standard Exception class to provide customized error handling with meaningful messages that can be displayed to users or logged for debugging.
 */
public class OurException extends Exception
{

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new OurException with the specified detail message. The message should describe the specific error condition that occurred and can be retrieved later via the getMessage() method.
     *
     * @param mensaje the detail message that explains the reason for the exception
     */
    public OurException(String mensaje)
    {
        super(mensaje);
    }
}
