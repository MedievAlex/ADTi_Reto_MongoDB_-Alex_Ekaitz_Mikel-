package unitTests;

import controller.Controller;
import dao.MockModelDAO;
import exception.OurException;
import model.Profile;
import model.User;
import model.Gender;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import java.util.ArrayList;

/**
 * Unit tests for the Controller class using MockModelDAO for isolation. These tests verify that the Controller properly delegates operations to the DAO and correctly handles both successful responses and exceptions.
 */
public class ControllerTest
{

    private Controller controller;
    private MockModelDAO mockDAO;

    /**
     * Sets up the test environment before each test method execution. Initializes a new MockModelDAO and Controller instance for each test.
     *
     * @throws exception.OurException
     */
    @Before
    public void setUp() throws OurException
    {
        mockDAO = new MockModelDAO();
        controller = new Controller(mockDAO);
    }

    /**
     * Tests user registration through the Controller. Verifies that the registered user matches the expected mock user data.
     *
     * @throws exception.OurException
     */
    @Test
    public void testRegisterUser() throws OurException
    {
        User testUser = new User("new@test.com", "newuser", "password123",
                "New", "User", "987654321", Gender.FEMALE, "9876543210987654");

        User result = controller.register(testUser);

        assertNotNull(result);
        assertEquals(1, result.getId());
        assertEquals("test@test.com", result.getEmail());
        assertEquals("testuser", result.getUsername());
    }

    /**
     * Tests user login functionality through the Controller. Verifies that login returns the expected user profile with correct data.
     *
     * @throws exception.OurException
     */
    @Test
    public void testLogin() throws OurException
    {
        Profile result = controller.login("testuser", "password");

        assertNotNull(result);
        assertEquals("testuser", result.getUsername());
        assertEquals("test@test.com", result.getEmail());
    }

    /**
     * Tests retrieval of all users through the Controller. Verifies that the user list is not empty and contains the expected mock user.
     *
     * @throws exception.OurException
     */
    @Test
    public void testGetUsers() throws OurException
    {
        ArrayList<User> result = controller.getUsers();

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(1, result.size());
        assertEquals("testuser", result.get(0).getUsername());
    }

    /**
     * Tests user update operation through the Controller. Verifies that the update operation returns true for successful updates.
     *
     * @throws exception.OurException
     */
    @Test
    public void testUpdateUser() throws OurException
    {
        User userToUpdate = new User(1, "updated@test.com", "updateduser", "newpass",
                "Updated", "Name", "111111111", Gender.MALE, "1234567890123456");

        boolean result = controller.updateUser(userToUpdate);
        assertTrue(result);
    }

    /**
     * Tests user deletion through the Controller. Verifies that deletion returns true for valid IDs and false for invalid IDs.
     *
     * @throws exception.OurException
     */
    @Test
    public void testDeleteUser() throws OurException
    {
        boolean result = controller.deleteUser(1);
        assertTrue(result);

        boolean result2 = controller.deleteUser(0);
        assertFalse(result2);
    }

    /**
     * Tests exception propagation from DAO to Controller. Verifies that exceptions thrown by the DAO are properly propagated.
     *
     * @throws exception.OurException
     */
    @Test(expected = OurException.class)
    public void testExceptionPropagation() throws OurException
    {
        mockDAO.setShouldThrowException(true, new OurException("Test exception"));
        controller.login("test", "test");
    }
}
