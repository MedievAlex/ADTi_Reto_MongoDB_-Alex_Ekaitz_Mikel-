package dao;

import exception.OurException;
import java.util.ArrayList;
import model.Gender;
import model.Profile;
import model.User;

/**
 * Mock implementation of ModelDAO for testing purposes. This class provides a simulated data access layer that can be configured to return predefined responses or throw exceptions for testing various application scenarios without requiring a real database connection.
 */
public class MockModelDAO implements ModelDAO
{

    private User mockUser;
    private Profile mockProfile;
    private final ArrayList<User> mockUsers;
    private boolean shouldThrowException;
    private OurException exceptionToThrow;

    /**
     * Constructs a new MockModelDAO with default test data. Initializes the mock with a sample user and empty exception state.
     */
    public MockModelDAO()
    {
        this.mockUser = new User(1, "test@test.com", "testuser", "Ab123456",
                "Test", "User", "123456789", Gender.MALE, "1234567890123456");
        this.mockProfile = mockUser;
        this.mockUsers = new ArrayList<>();
        mockUsers.add(mockUser);
        this.shouldThrowException = false;
    }

    /**
     * Configures the mock to throw an exception on subsequent method calls.
     *
     * @param shouldThrow if true, subsequent method calls will throw the specified exception
     * @param exception the exception to throw when shouldThrow is true
     */
    public void setShouldThrowException(boolean shouldThrow, OurException exception)
    {
        this.shouldThrowException = shouldThrow;
        this.exceptionToThrow = exception;
    }

    /**
     * Sets the mock user and profile to be returned by subsequent method calls.
     *
     * @param user the User object to set as the mock response for user operations
     */
    public void setMockUser(User user)
    {
        this.mockUser = user;
        this.mockProfile = user;
    }

    /**
     * Simulates user registration by returning a predefined mock user.
     *
     * @param user the user to register (ignored in mock implementation)
     * @return the predefined mock user
     * @throws OurException if configured to throw exceptions
     */
    @Override
    public User register(User user) throws OurException
    {
        if (shouldThrowException)
        {
            throw exceptionToThrow;
        }
        return mockUser;
    }

    /**
     * Simulates user login by returning a predefined mock profile.
     *
     * @param credential the user credential (ignored in mock implementation)
     * @param password the user password (ignored in mock implementation)
     * @return the predefined mock profile
     * @throws OurException if configured to throw exceptions
     */
    @Override
    public Profile login(String credential, String password) throws OurException
    {
        if (shouldThrowException)
        {
            throw exceptionToThrow;
        }
        return mockProfile;
    }

    /**
     * Simulates retrieving all users by returning a predefined list of mock users.
     *
     * @return an ArrayList containing the predefined mock users
     * @throws OurException if configured to throw exceptions
     */
    @Override
    public ArrayList<User> getUsers() throws OurException
    {
        if (shouldThrowException)
        {
            throw exceptionToThrow;
        }
        return mockUsers;
    }

    /**
     * Simulates user update operation by always returning true.
     *
     * @param user the user to update (ignored in mock implementation)
     * @return always returns true
     * @throws OurException if configured to throw exceptions
     */
    @Override
    public boolean updateUser(User user) throws OurException
    {
        if (shouldThrowException)
        {
            throw exceptionToThrow;
        }
        return true;
    }

    /**
     * Simulates user deletion by returning true for positive IDs.
     *
     * @param id the user ID to delete
     * @return true if id is greater than 0, false otherwise
     * @throws OurException if configured to throw exceptions
     */
    @Override
    public boolean deleteUser(int id) throws OurException
    {
        if (shouldThrowException)
        {
            throw exceptionToThrow;
        }
        return id > 0;
    }
}
