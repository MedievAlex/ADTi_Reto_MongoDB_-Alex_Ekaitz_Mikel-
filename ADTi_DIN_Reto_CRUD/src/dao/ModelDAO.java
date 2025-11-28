package dao;

import exception.OurException;
import java.util.ArrayList;
import model.Profile;
import model.User;

/**
 * Data Access Object interface defining the contract for all data operations in the application. This interface specifies the methods required for user management, authentication, and profile operations that must be implemented by any data access implementation.
 *
 * <p>
 * The interface follows the DAO pattern to separate business logic from data persistence details, allowing for different database implementations while maintaining consistent application behavior.</p>
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public interface ModelDAO
{

    /**
     * Retrieves a list of all users from the data store. This method should return all user records with their complete profile information, typically used for administrative purposes or user management.
     *
     * @return an ArrayList containing all User objects in the system
     * @throws OurException if the user retrieval operation fails due to data access errors, connectivity issues, or system failures
     */
    public ArrayList<User> getUsers() throws OurException;

    /**
     * Updates an existing user's information in the data store. This method should persist changes made to a user's profile data, ensuring that all modifications are saved and reflected in the storage.
     *
     * @param user the User object containing updated information to be saved
     * @return true if the update operation was successful and affected at least one record, false if no changes were made or no user was found
     * @throws OurException if the update operation fails due to validation errors, data integrity constraints, data access issues, or system failures
     */
    public boolean updateUser(User user) throws OurException;

    /**
     * Deletes a user from the data store by their unique identifier. This method should permanently remove a user record from the system based on the provided user ID.
     *
     * @param id the unique identifier of the user to be deleted
     * @return true if the deletion was successful and affected at least one record, false if no user was found with the specified ID
     * @throws OurException if the deletion operation fails due to data integrity constraints, referential integrity issues, data access errors, or system failures
     */
    public boolean deleteUser(int id) throws OurException;

    /**
     * Authenticates a user using provided credentials. This method should verify user identity by checking the provided credential (which can be username or email) and password against stored user data.
     *
     * @param credential the user's username or email address used for identification
     * @param password the user's password for authentication
     * @return the authenticated user's Profile object containing user information and access privileges if credentials are valid, null if authentication fails due to invalid credentials
     * @throws OurException if authentication fails due to system errors, data access issues, or unexpected failures during the process
     */
    public Profile login(String credential, String password) throws OurException;

    /**
     * Registers a new user in the system. This method should create a new user account with the provided information, assign a unique identifier, and persist the user data to the storage system.
     *
     * @param user the User object containing all registration information
     * @return the registered User object, typically with the generated identifier and any system-assigned values
     * @throws OurException if the registration process fails due to duplicate credentials, validation errors, data integrity constraints, data access issues, or system failures
     */
    public User register(User user) throws OurException;
}
