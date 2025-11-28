package unitTests;

import exception.OurException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test class for OurException
 */
public class OurExceptionTest
{

    /**
     * Tests constructor with message
     */
    @Test
    public void testConstructorWithMessage()
    {
        String errorMessage = "Test error message";
        OurException exception = new OurException(errorMessage);

        assertEquals(errorMessage, exception.getMessage());
    }

    /**
     * Tests constructor with empty message
     */
    @Test
    public void testConstructorWithEmptyMessage()
    {
        OurException exception = new OurException("");

        assertEquals("", exception.getMessage());
    }

    /**
     * Tests constructor with null message
     */
    @Test
    public void testConstructorWithNullMessage()
    {
        OurException exception = new OurException(null);

        assertNull(exception.getMessage());
    }

    /**
     * Tests exception inheritance
     */
    @Test
    public void testExceptionInheritance()
    {
        OurException exception = new OurException("Test message");

        assertTrue(exception instanceof Exception);
    }

    /**
     * Tests exception with special characters in message
     */
    @Test
    public void testConstructorWithSpecialCharacters()
    {
        String message = "Error: Invalid input @#$%^&*()";
        OurException exception = new OurException(message);

        assertEquals(message, exception.getMessage());
    }

    /**
     * Tests exception with long message
     */
    @Test
    public void testConstructorWithLongMessage()
    {
        String longMessage = "This is a very long error message that describes "
                + "a complex error condition that might occur when "
                + "multiple validation rules fail simultaneously";
        OurException exception = new OurException(longMessage);

        assertEquals(longMessage, exception.getMessage());
    }
}
