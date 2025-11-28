package controller;

import java.io.IOException;
import java.util.ArrayList;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import dao.DBImplementation;
import dao.ModelDAO;
import exception.ErrorMessages;
import exception.OurException;
import model.Profile;
import model.User;
import javafx.scene.image.Image;

/**
 * Main controller class that serves as the central coordinator between the user interface and the data access layer. This class handles application initialization, window management, and delegates business operations to the underlying DAO implementation.
 *
 * The controller follows the Facade pattern by providing a simplified interface to complex subsystem operations while managing the application's main workflow and user navigation.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class Controller
{

    private final ModelDAO dao;

    /**
     * Constructs a new Controller instance with a custom DAO implementation. This constructor is primarily intended for testing purposes, allowing dependency injection of mock or test DAO implementations.
     *
     * @param dao the ModelDAO implementation to use for data access operations
     */
    public Controller(ModelDAO dao)
    {
        this.dao = dao;
    }

    /**
     * Constructs a new Controller instance and initializes the data access layer. This constructor attempts to establish a connection to the database through the DBImplementation class. If the database connection fails, an exception is thrown with a descriptive error message.
     *
     * @throws OurException if the database connection cannot be established, containing details about the connection failure
     */
    public Controller() throws OurException
    {
        try
        {
            dao = new DBImplementation();
        }
        catch (Exception ex)
        {
            throw new OurException(ErrorMessages.DATABASE);
        }
    }

    /**
     * Displays the main application window starting with the login interface. This method initializes the primary stage with the login window scene, configures the window properties including title and icon, and sets up the controller reference for the login interface.
     *
     * @param stage the primary stage to be used for displaying the application window
     * @throws IOException if the FXML file for the login window cannot be loaded or if there are issues with the resource loading process
     */
    public void showWindow(Stage stage) throws IOException
    {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/LoginWindow.fxml"));
        Parent root = loader.load();

        LoginWindowController loginController = loader.getController();
        loginController.setController(this);

        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.setTitle("Log In");
        stage.setResizable(false);
        stage.getIcons().add(new Image(getClass().getResourceAsStream("/images/logo.png")));
        stage.show();
    }

    /**
     * Registers a new user in the system. This method delegates the user registration process to the data access layer, which handles the persistence of user data and ensures data integrity.
     *
     * @param user the User object containing all registration information
     * @return the registered User object, typically with generated identifiers and any system-assigned values
     * @throws OurException if the registration process fails due to validation errors, duplicate users, or database constraints violations
     */
    public User register(User user) throws OurException
    {
        return dao.register(user);
    }

    /**
     * Authenticates a user using provided credentials. This method verifies user identity by checking the provided credential (which can be username or email) and password against stored user data.
     *
     * @param credential the user's username or email address used for identification
     * @param password the user's password for authentication
     * @return the authenticated user's Profile object containing user information and access privileges
     * @throws OurException if authentication fails due to invalid credentials, user not found, or database access issues
     */
    public Profile login(String credential, String password) throws OurException
    {
        return dao.login(credential, password);
    }

    /**
     * Retrieves a list of all users from the system. This method provides access to the complete user database, typically used by administrative interfaces for user management operations.
     *
     * @return an ArrayList containing all User objects in the system
     * @throws OurException if the user retrieval operation fails due to database connectivity issues or data access errors
     */
    public ArrayList<User> getUsers() throws OurException
    {
        return dao.getUsers();
    }

    /**
     * Updates an existing user's information in the system. This method persists changes made to a user's profile data, ensuring that modifications are saved to the database.
     *
     * @param user the User object containing updated information to be saved
     * @return true if the update operation was successful, false if no changes were made or the operation did not affect any records
     * @throws OurException if the update operation fails due to validation errors, database constraints violations, or data access issues
     */
    public boolean updateUser(User user) throws OurException
    {
        return dao.updateUser(user);
    }

    /**
     * Deletes a user from the system by their unique identifier. This method permanently removes a user record from the database based on the provided user ID.
     *
     * @param id the unique identifier of the user to be deleted
     * @return true if the deletion was successful, false if no user was found with the specified ID or the operation did not affect any records
     * @throws OurException if the deletion operation fails due to database constraints, referential integrity issues, or data access errors
     */
    public boolean deleteUser(int id) throws OurException
    {
        return dao.deleteUser(id);
    }
}
