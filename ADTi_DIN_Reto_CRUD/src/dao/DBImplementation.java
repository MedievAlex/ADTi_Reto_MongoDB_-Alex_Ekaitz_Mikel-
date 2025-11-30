package dao;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import exception.OurException;
import exception.ErrorMessages;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import model.Admin;
import model.Gender;
import model.LoggedProfile;
import model.Profile;
import model.User;
import org.bson.Document;
import org.bson.types.ObjectId;
import pool.ConnectionPool;

/**
 * Database implementation of the ModelDAO interface. This class provides the concrete implementation for all data access operations including user registration, authentication, profile management, and administrative functions. It handles database connections, SQL execution, transaction management, and error handling for the entire application data layer.
 *
 * The class implements connection pooling with timeout mechanisms and ensures proper transaction handling for atomic operations.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class DBImplementation implements ModelDAO
{
    private final MongoDatabase DB;
    private final MongoCollection<Document> PROFILES;
    
    public DBImplementation()
    {
        DB = ConnectionPool.getDatabase();
        PROFILES = DB.getCollection("profiles");
    }

    /**
     * Inserts a new user into the database with transaction support. This method performs an atomic operation that inserts user data into both the profile and user tables within a single transaction. If any part fails, the entire transaction is rolled back.
     *
     * @param con the database connection to use for the operation
     * @param user the User object containing all user data to be inserted
     * @return the generated user ID if insertion is successful, -1 otherwise
     * @throws OurException if the insertion fails due to SQL errors, constraint violations, or transaction issues
     */
    private String insert(User user) throws OurException
    {
        try
        {
            Document userDocument = new Document()
            .append("type", "User")
            .append("email", user.getEmail())
            .append("username", user.getUsername())
            .append("password", user.getPassword())
            .append("name", user.getName())
            .append("lastname", user.getLastname())
            .append("telephone", user.getTelephone())
            .append("gender", user.getGender().name())
            .append("card", user.getCard());

            PROFILES.insertOne(userDocument);

            return userDocument.getObjectId("_id").toHexString();
        }
        catch (MongoException ex)
        {
            throw new OurException(ErrorMessages.REGISTER_USER);
        }
    }

    /**
     * Retrieves all users from the database. This method executes a query to fetch all user records with their complete profile information including personal details and preferences.
     *
     * @param con the database connection to use for the operation
     * @return an ArrayList containing all User objects from the database
     * @throws OurException if the query execution fails or data retrieval errors occur
     */
    private ArrayList<User> selectUsers() throws OurException
    {
        ArrayList<User> users = new ArrayList<>();

        try
        {
            FindIterable<Document> documents = PROFILES.find(eq("type", "User"));
            
            for (Document doc : documents) {
                User user = new User(doc.getObjectId("_id").toHexString(), doc.getString("email"), doc.getString("username"), doc.getString("password"), doc.getString("name"), doc.getString("lastname"), doc.getString("telephone"), Gender.valueOf(doc.getString("gender")), doc.getString("card"));
                
                users.add(user);
            }
            
            return users;
        }
        catch (MongoException ex)
        {
            throw new OurException(ErrorMessages.GET_USERS);
        }
    }

    /**
     * Updates an existing user's information in the database with transaction support. This method performs an atomic operation that updates user data in both the profile and user tables within a single transaction.
     *
     * @param con the database connection to use for the operation
     * @param user the User object containing updated user data
     * @return true if the update operation was successful, false otherwise
     * @throws OurException if the update fails due to SQL errors, constraint violations, or transaction issues
     */
    private boolean update(User user) throws OurException
    {
        try
        {
            ObjectId objectId = new ObjectId(user.getId());
        
            UpdateResult result = PROFILES.updateOne(
                eq("_id", objectId),
                combine(
                    set("password", user.getPassword()),
                    set("name", user.getName()),
                    set("lastname", user.getLastname()),
                    set("telephone", user.getTelephone()),
                    set("gender", user.getGender().name()),
                    set("card", user.getCard()))
            );

            return result.getModifiedCount() > 0;
        }
        catch (MongoException ex)
        {
            throw new OurException(ErrorMessages.UPDATE_USER);
        }
    }

    /**
     * Deletes a user from the database by their unique identifier. This method removes a user record from the system based on the provided user ID.
     *
     * @param con the database connection to use for the operation
     * @param userId the unique identifier of the user to be deleted
     * @return true if the deletion was successful, false if no user was found with the specified ID
     * @throws OurException if the deletion operation fails due to SQL errors or database constraints
     */
    private boolean delete(String id) throws OurException
    {
        try
        {
            ObjectId objectId = new ObjectId(id);
        
            DeleteResult result = PROFILES.deleteOne(eq("_id", objectId));
        
            return result.getDeletedCount() > 0;
        }
        catch (MongoException ex)
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
    private Profile loginProfile(String credential, String password) throws OurException
    {
        try
        {
            Document doc = PROFILES.find(or(eq("email", credential),eq("username", credential))).first();
            
            if (doc == null || !password.equals(doc.getString("password")))
            {
                return null;
            }

            String type = doc.getString("type");

            if ("User".equals(type))
            {
                return new User(doc.getObjectId("_id").toHexString(), doc.getString("email"), doc.getString("username"), doc.getString("password"), doc.getString("name"), doc.getString("lastname"), doc.getString("telephone"), Gender.valueOf(doc.getString("gender")), doc.getString("card"));
            } else if ("Admin".equals(type))
            {
                return new Admin(doc.getObjectId("_id").toHexString(), doc.getString("email"), doc.getString("username"), doc.getString("password"), doc.getString("name"), doc.getString("lastname"), doc.getString("telephone"), doc.getString("currentAccount"));
            }
            
            return null;
        }
        catch (MongoException ex)
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
    private HashMap<String, Boolean> checkCredentialsExistence(String email, String username) throws OurException {
        HashMap<String, Boolean> exists = new HashMap<>();

        try {
            long emailCount = PROFILES.countDocuments(eq("email", email));
            exists.put("email", emailCount > 0);

            long usernameCount = PROFILES.countDocuments(eq("username", username));
            exists.put("username", usernameCount > 0);

            return exists;

        } catch (Exception ex) {
            throw new OurException(ErrorMessages.VERIFY_CREDENTIALS);
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
        Profile profile = loginProfile(credential, password);

        if (profile != null)
        {
            LoggedProfile.getInstance().setProfile(profile);
        }

        return profile;
    }

    /**
     * Registers a new user in the system with duplicate credential checking. This method validates credential uniqueness, creates a new user account, and returns the registered user with their system-generated identifier.
     *
     * @param user the User object containing all registration information
     * @return the registered User object with the generated ID and system-assigned values
     * @throws OurException if registration fails due to duplicate credentials, database constraints, or system errors
     */
    @Override
    public User register(User user) throws OurException {
        Map<String, Boolean> existing = checkCredentialsExistence(user.getEmail(), user.getUsername());
        String id;

        if (existing.get("email") && existing.get("username")) {
            throw new OurException("Both email and username already exist");
        } else if (existing.get("email")) {
            throw new OurException("Email already exists");
        } else if (existing.get("username")) {
            throw new OurException("Username already exists");
        }

        id = insert(user);

        user.setId(id);

        return user;
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
       return selectUsers();
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
        return update(user);
    }

    /**
     * Deletes a user from the system by their unique identifier. This method permanently removes a user record from the database based on the provided user ID.
     *
     * @param id the unique identifier of the user to be deleted
     * @return true if the deletion was successful, false if no user was found with the specified ID or the operation did not affect any records
     * @throws OurException if the deletion operation fails due to database constraints, referential integrity issues, or data access errors
     */
    @Override
    public boolean deleteUser(String id) throws OurException
    {
        return delete(id);
    }
}
