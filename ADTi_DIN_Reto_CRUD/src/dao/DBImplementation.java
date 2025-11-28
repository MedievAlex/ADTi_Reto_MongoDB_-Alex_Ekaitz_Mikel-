package dao;

import exception.OurException;
import exception.ErrorMessages;
import pool.ConnectionPool;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import model.Admin;
import model.Gender;
import model.LoggedProfile;
import model.Profile;
import model.User;
import pool.ConnectionThread;

/**
 * Database implementation of the ModelDAO interface. This class provides the concrete implementation for all data access operations including user registration, authentication, profile management, and administrative functions. It handles database connections, SQL execution, transaction management, and error handling for the entire application data layer.
 *
 * The class implements connection pooling with timeout mechanisms and ensures proper transaction handling for atomic operations.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class DBImplementation implements ModelDAO
{

    private final int delay = 30;

    /**
     * SQL Queries: INSERTS
     */
    private final String SQLINSERT_PROFILE = "INSERT INTO db_profile (P_EMAIL, P_USERNAME, P_PASSWORD, P_NAME, P_LASTNAME, P_TELEPHONE) VALUES (?, ?, ?, ?, ?, ?)";
    private static final String SQLINSERT_USER = "INSERT INTO db_user (U_ID, U_GENDER, U_CARD) VALUES (?, ?, ?)";

    /**
     * SQL Queries: SELECTS
     */
    private final String SQLSELECT_USERS = "SELECT p.P_ID, p.P_EMAIL, p.P_USERNAME, p.P_PASSWORD, p.P_NAME, p.P_LASTNAME, p.P_TELEPHONE, u.U_GENDER, u.U_CARD FROM db_profile p JOIN db_user u ON p.P_ID = u.U_ID";
    private final String SQLCHECK_CREDENTIALS = "SELECT P_EMAIL, P_USERNAME FROM db_profile WHERE P_EMAIL = ? OR P_USERNAME = ?";
    private final String SQLSELECT_LOGIN = "SELECT p.P_ID, p.P_EMAIL, p.P_USERNAME, p.P_PASSWORD, p.P_NAME, p.P_LASTNAME, p.P_TELEPHONE, u.U_GENDER, u.U_CARD, a.A_CURRENT_ACCOUNT FROM db_profile p LEFT JOIN db_user u ON p.P_ID = u.U_ID LEFT JOIN db_admin a ON p.P_ID = a.A_ID WHERE (p.P_EMAIL = ? OR p.P_USERNAME = ?) AND p.P_PASSWORD = ?";

    /**
     * SQL Queries: UPDATES
     */
    private final String SQLUPDATE_PROFILE = "UPDATE db_profile SET P_PASSWORD = ?, P_NAME = ?, P_LASTNAME = ?, P_TELEPHONE = ? WHERE P_ID = ?";
    private final String SQLUPDATE_USER = "UPDATE db_user SET U_GENDER = ?, U_CARD = ? WHERE U_ID = ?";

    /**
     * SQL Queries: DELETES
     */
    private final String SQLDELETE_USER = "DELETE FROM db_profile WHERE P_ID = ?";

    /**
     * Inserts a new user into the database with transaction support. This method performs an atomic operation that inserts user data into both the profile and user tables within a single transaction. If any part fails, the entire transaction is rolled back.
     *
     * @param con the database connection to use for the operation
     * @param user the User object containing all user data to be inserted
     * @return the generated user ID if insertion is successful, -1 otherwise
     * @throws OurException if the insertion fails due to SQL errors, constraint violations, or transaction issues
     */
    private int insert(Connection con, User user) throws OurException
    {
        int id = -1;

        try (
                PreparedStatement stmtProfile = con.prepareStatement(SQLINSERT_PROFILE, Statement.RETURN_GENERATED_KEYS);
                PreparedStatement stmtUser = con.prepareStatement(SQLINSERT_USER))
        {
            con.setAutoCommit(false);

            stmtProfile.setString(1, user.getEmail());
            stmtProfile.setString(2, user.getUsername());
            stmtProfile.setString(3, user.getPassword());
            stmtProfile.setString(4, user.getName());
            stmtProfile.setString(5, user.getLastname());
            stmtProfile.setString(6, user.getTelephone());

            int profileInserted = stmtProfile.executeUpdate();

            if (profileInserted == 0)
            {
                throw new SQLException("Insert failed: profile not created.");
            }

            try (ResultSet generatedKeys = stmtProfile.getGeneratedKeys())
            {
                if (generatedKeys.next())
                {
                    id = generatedKeys.getInt(1);

                    stmtUser.setInt(1, id);
                    stmtUser.setString(2, user.getGender().name());
                    stmtUser.setString(3, user.getCard());

                    int userInserted = stmtUser.executeUpdate();

                    if (userInserted == 0)
                    {
                        throw new SQLException("Insert failed: user not created.");
                    }

                    con.commit();
                }
                else
                {
                    throw new SQLException("Insert failed: no generated key returned.");
                }
            }
        }
        catch (SQLException ex)
        {
            rollBack(con);
            throw new OurException(ErrorMessages.REGISTER_USER);
        } finally
        {
            resetAutoCommit(con);
        }

        return id;
    }

    /**
     * Retrieves all users from the database. This method executes a query to fetch all user records with their complete profile information including personal details and preferences.
     *
     * @param con the database connection to use for the operation
     * @return an ArrayList containing all User objects from the database
     * @throws OurException if the query execution fails or data retrieval errors occur
     */
    private ArrayList<User> selectUsers(Connection con) throws OurException
    {
        ArrayList<User> users = new ArrayList<>();

        try (
                PreparedStatement stmt = con.prepareStatement(SQLSELECT_USERS);
                ResultSet rs = stmt.executeQuery())
        {
            while (rs.next())
            {
                String genderValue = rs.getString("U_GENDER");
                Gender gender = genderValue != null ? Gender.valueOf(genderValue) : Gender.OTHER;
                User user = new User(
                        rs.getInt("P_ID"),
                        rs.getString("P_EMAIL"),
                        rs.getString("P_USERNAME"),
                        rs.getString("P_PASSWORD"),
                        rs.getString("P_NAME"),
                        rs.getString("P_LASTNAME"),
                        rs.getString("P_TELEPHONE"),
                        gender,
                        rs.getString("U_CARD")
                );
                users.add(user);
            }
        }
        catch (SQLException ex)
        {
            throw new OurException(ErrorMessages.GET_USERS);
        }
        return users;
    }

    /**
     * Updates an existing user's information in the database with transaction support. This method performs an atomic operation that updates user data in both the profile and user tables within a single transaction.
     *
     * @param con the database connection to use for the operation
     * @param user the User object containing updated user data
     * @return true if the update operation was successful, false otherwise
     * @throws OurException if the update fails due to SQL errors, constraint violations, or transaction issues
     */
    private boolean update(Connection con, User user) throws OurException
    {
        boolean success = false;

        try (
                PreparedStatement stmtProfile = con.prepareStatement(SQLUPDATE_PROFILE);
                PreparedStatement stmtUser = con.prepareStatement(SQLUPDATE_USER))
        {
            con.setAutoCommit(false);

            stmtProfile.setString(1, user.getPassword());
            stmtProfile.setString(2, user.getName());
            stmtProfile.setString(3, user.getLastname());
            stmtProfile.setString(4, user.getTelephone());
            stmtProfile.setInt(5, user.getId());

            int profileUpdated = stmtProfile.executeUpdate();

            stmtUser.setString(1, user.getGender().name());
            stmtUser.setString(2, user.getCard());
            stmtUser.setInt(3, user.getId());

            int userUpdated = stmtUser.executeUpdate();

            if (profileUpdated == 0 || userUpdated == 0)
            {
                throw new SQLException(ErrorMessages.UPDATE_USER);
            }

            con.commit();
            success = true;
        }
        catch (SQLException ex)
        {
            rollBack(con);
            throw new OurException(ErrorMessages.UPDATE_USER);
        } finally
        {
            resetAutoCommit(con);
        }

        return success;
    }

    /**
     * Deletes a user from the database by their unique identifier. This method removes a user record from the system based on the provided user ID.
     *
     * @param con the database connection to use for the operation
     * @param userId the unique identifier of the user to be deleted
     * @return true if the deletion was successful, false if no user was found with the specified ID
     * @throws OurException if the deletion operation fails due to SQL errors or database constraints
     */
    private boolean delete(Connection con, int userId) throws OurException
    {
        try (PreparedStatement stmt = con.prepareStatement(SQLDELETE_USER))
        {
            stmt.setInt(1, userId);
            return stmt.executeUpdate() > 0;
        }
        catch (SQLException ex)
        {
            throw new OurException(ErrorMessages.DELETE_USER);
        }
    }

    /**
     * Authenticates a user by verifying credentials against the database. This method checks if the provided credential (email or username) and password match an existing user record and returns the appropriate profile type (User or Admin) upon successful authentication.
     *
     * @param con the database connection to use for the operation
     * @param credential the user's email or username for identification
     * @param password the user's password for authentication
     * @return the authenticated user's Profile object (User or Admin) if credentials are valid, null otherwise
     * @throws OurException if the authentication process fails due to SQL errors or data retrieval issues
     */
    private Profile loginProfile(Connection con, String credential, String password) throws OurException
    {
        try (PreparedStatement stmt = con.prepareStatement(SQLSELECT_LOGIN))
        {
            stmt.setString(1, credential);
            stmt.setString(2, credential);
            stmt.setString(3, password);

            try (ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    String gender = rs.getString("U_GENDER");
                    String admin = rs.getString("A_CURRENT_ACCOUNT");

                    if (gender != null)
                    {
                        return new User(
                                rs.getInt("P_ID"),
                                rs.getString("P_EMAIL"),
                                rs.getString("P_USERNAME"),
                                rs.getString("P_PASSWORD"),
                                rs.getString("P_NAME"),
                                rs.getString("P_LASTNAME"),
                                rs.getString("P_TELEPHONE"),
                                Gender.valueOf(gender),
                                rs.getString("U_CARD")
                        );
                    }
                    else if (admin != null)
                    {
                        return new Admin(
                                rs.getInt("P_ID"),
                                rs.getString("P_EMAIL"),
                                rs.getString("P_USERNAME"),
                                rs.getString("P_PASSWORD"),
                                rs.getString("P_NAME"),
                                rs.getString("P_LASTNAME"),
                                rs.getString("P_TELEPHONE"),
                                admin
                        );
                    }
                }

                return null;
            }
        }
        catch (SQLException ex)
        {
            throw new OurException(ErrorMessages.LOGIN);
        }
    }

    /**
     * Checks if the provided email or username already exists in the database. This method verifies the uniqueness of user credentials during registration to prevent duplicate accounts.
     *
     * @param con the database connection to use for the operation
     * @param email the email address to check for existence
     * @param username the username to check for existence
     * @return a HashMap indicating which credentials already exist with keys "email" and "username" and boolean values
     * @throws OurException if the verification process fails due to SQL errors
     */
    private HashMap<String, Boolean> checkCredentialsExistence(Connection con, String email, String username) throws OurException
    {
        HashMap<String, Boolean> exists = new HashMap<>();
        exists.put("email", false);
        exists.put("username", false);

        try (PreparedStatement stmt = con.prepareStatement(SQLCHECK_CREDENTIALS))
        {
            stmt.setString(1, email);
            stmt.setString(2, username);

            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    if (email.equals(rs.getString("P_EMAIL")))
                    {
                        exists.put("email", true);
                    }
                    if (username.equals(rs.getString("P_USERNAME")))
                    {
                        exists.put("username", true);
                    }
                }
            }
        }
        catch (SQLException ex)
        {
            throw new OurException(ErrorMessages.VERIFY_CREDENTIALS);
        }
        return exists;
    }

    /**
     * Waits for a database connection to become available with timeout protection. This method implements a polling mechanism to wait for a connection thread to become ready, preventing indefinite blocking.
     *
     * @param thread the ConnectionThread instance to wait for
     * @return the established database connection
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws OurException if the connection timeout is exceeded
     */
    private Connection waitForConnection(ConnectionThread thread) throws InterruptedException, OurException
    {
        int attempts = 0;

        while (!thread.isReady() && attempts < 50)
        {
            Thread.sleep(10);
            attempts++;
        }

        if (!thread.isReady())
        {
            throw new OurException(ErrorMessages.TIMEOUT);
        }

        return thread.getConnection();
    }

    /**
     * Rolls back the current database transaction. This method provides safe transaction rollback with proper error handling for scenarios where database operations fail.
     *
     * @param con the database connection to roll back the transaction on
     * @throws OurException if the rollback operation fails
     */
    private void rollBack(Connection con) throws OurException
    {
        try
        {
            if (con != null)
            {
                con.rollback();
            }
        }
        catch (SQLException ex)
        {
            throw new OurException(ErrorMessages.ROLLBACK);
        }
    }

    /**
     * Resets the auto-commit mode of the database connection to true. This method ensures that the connection returns to its default auto-commit state after transaction operations are completed.
     *
     * @param con the database connection to reset
     * @throws OurException if resetting auto-commit fails
     */
    private void resetAutoCommit(Connection con) throws OurException
    {
        try
        {
            if (con != null)
            {
                con.setAutoCommit(true);
            }
        }
        catch (SQLException ex)
        {
            throw new OurException(ErrorMessages.RESET_AUTOCOMMIT);
        }
    }

    /**
     * Authenticates a user with the provided credentials. This method verifies user identity by checking the provided credential and password against stored user data and sets the logged-in profile upon successful authentication.
     *
     * @param credential the user's username or email address used for identification
     * @param password the user's password for authentication
     * @return the authenticated user's Profile object containing user information and access privileges, or null if authentication fails
     * @throws OurException if authentication fails due to database errors or system issues
     */
    @Override
    public Profile login(String credential, String password) throws OurException
    {
        try (Connection con = ConnectionPool.getConnection())
        {
            Profile profile = loginProfile(con, credential, password);
            if (profile != null)
            {
                LoggedProfile.getInstance().setProfile(profile);
            }
            return profile;
        }
        catch (Exception ex)
        {
            throw new OurException(ErrorMessages.LOGIN);
        }
    }

    /**
     * Registers a new user in the system with duplicate credential checking. This method validates credential uniqueness, creates a new user account, and returns the registered user with their system-generated identifier.
     *
     * @param user the User object containing all registration information
     * @return the registered User object with the generated ID and system-assigned values
     * @throws OurException if registration fails due to duplicate credentials, database constraints, or system errors
     */
    @Override
    public User register(User user) throws OurException
    {
        ConnectionThread thread = new ConnectionThread(delay);
        thread.start();

        try
        {
            Connection con = waitForConnection(thread);
            Map<String, Boolean> existing = checkCredentialsExistence(con, user.getEmail(), user.getUsername());

            if (existing.get("email") && existing.get("username"))
            {
                throw new OurException("Both email and username already exist");
            }
            else if (existing.get("email"))
            {
                throw new OurException("Email already exists");
            }
            else if (existing.get("username"))
            {
                throw new OurException("Username already exists");
            }

            int id = insert(con, user);

            if (id == -1)
            {
                throw new OurException(ErrorMessages.REGISTER_USER);
            }

            user.setId(id);

            return user;
        }
        catch (InterruptedException ex)
        {
            throw new OurException(ErrorMessages.REGISTER_USER);
        } finally
        {
            thread.releaseConnection();
        }
    }

    /**
     * Retrieves a list of all users from the system. This method provides access to the complete user database, typically used by administrative interfaces for user management operations.
     *
     * @return an ArrayList containing all User objects in the system
     * @throws OurException if the user retrieval operation fails due to database connectivity issues or data access errors
     */
    @Override
    public ArrayList<User> getUsers() throws OurException
    {
        ConnectionThread thread = new ConnectionThread(delay);
        thread.start();

        try
        {
            Connection con = waitForConnection(thread);
            return selectUsers(con);
        }
        catch (InterruptedException ex)
        {
            throw new OurException(ErrorMessages.GET_USERS);
        } finally
        {
            thread.releaseConnection();
        }
    }

    /**
     * Updates an existing user's information in the system. This method persists changes made to a user's profile data, ensuring that modifications are saved to the database.
     *
     * @param user the User object containing updated information to be saved
     * @return true if the update operation was successful, false if no changes were made or the operation did not affect any records
     * @throws OurException if the update operation fails due to validation errors, database constraints violations, or data access issues
     */
    @Override
    public boolean updateUser(User user) throws OurException
    {
        ConnectionThread thread = new ConnectionThread(delay);
        thread.start();

        try
        {
            Connection con = waitForConnection(thread);
            return update(con, user);
        }
        catch (InterruptedException ex)
        {
            throw new OurException(ErrorMessages.UPDATE_USER);
        } finally
        {
            thread.releaseConnection();
        }
    }

    /**
     * Deletes a user from the system by their unique identifier. This method permanently removes a user record from the database based on the provided user ID.
     *
     * @param id the unique identifier of the user to be deleted
     * @return true if the deletion was successful, false if no user was found with the specified ID or the operation did not affect any records
     * @throws OurException if the deletion operation fails due to database constraints, referential integrity issues, or data access errors
     */
    @Override
    public boolean deleteUser(int id) throws OurException
    {
        ConnectionThread thread = new ConnectionThread(delay);
        thread.start();

        try
        {
            Connection con = waitForConnection(thread);
            return delete(con, id);
        }
        catch (InterruptedException ex)
        {
            throw new OurException(ErrorMessages.DELETE_USER);
        } finally
        {
            thread.releaseConnection();
        }
    }
}
