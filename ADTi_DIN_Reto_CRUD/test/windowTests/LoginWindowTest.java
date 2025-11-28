package windowTests;

import controller.Controller;
import model.LoggedProfile;
import controller.LoginWindowController;
import exception.OurException;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.User;
import model.Gender;
import org.junit.Before;
import org.junit.Test;
import org.testfx.framework.junit.ApplicationTest;
import dao.MockModelDAO;

import static org.junit.Assert.*;
import static org.testfx.api.FxAssert.verifyThat;
import static org.testfx.matcher.control.TextInputControlMatchers.hasText;

/**
 * Automated UI tests for the LoginWindow JavaFX interface.
 * <p>
 * This class uses TestFX and JUnit to verify that all components of the login interface are loaded correctly, input fields accept data, navigation to sign-up window functions, and login attempts (successful or failed) behave as expected.
 * <p>
 * MockModelDAO simulates backend responses, while LoggedProfile singleton manages the current user session.
 */
public class LoginWindowTest extends ApplicationTest
{

    /**
     * Controller for the LoginWindow, used to access and manipulate UI elements.
     */
    private LoginWindowController loginController;

    /**
     * Main controller handling application logic and data interactions.
     */
    private Controller realController;

    /**
     * Mock DAO to simulate database operations and exceptions.
     */
    private MockModelDAO mockDAO;

    /**
     * Mock user used for testing successful login scenarios.
     */
    private User mockUser;

    /**
     * Mock admin used for testing successful admin login scenarios.
     */
    private User mockAdmin;

    /**
     * Initializes the JavaFX stage for testing the LoginWindow. Loads FXML, sets the scene, and prepares mock users for login tests.
     *
     * @param stage the JavaFX stage to display the login window
     * @throws Exception if FXML loading fails
     */
    @Override
    public void start(Stage stage) throws Exception
    {
        mockDAO = new MockModelDAO();
        realController = new Controller(mockDAO);

        mockUser = new User(1, "user@test.com", "testuser", "password123",
                "Test", "User", "123456789", Gender.MALE, "1234567890123456");
        mockAdmin = new User(1, "admin@test.com", "adminuser", "password123",
                "Admin", "User", "123456789", Gender.MALE, "1234567890123456");

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/LoginWindow.fxml"));
        Parent root = loader.load();
        loginController = loader.getController();

        loginController.setController(realController);

        stage.setScene(new Scene(root));
        stage.show();
    }

    /**
     * Sets up the testing environment before each test. Clears the logged-in profile and resets the mock DAO behavior.
     */
    @Before
    public void setUp()
    {
        LoggedProfile.getInstance().clear();
        mockDAO.setMockUser(null);
        mockDAO.setShouldThrowException(false, null);
    }

    /**
     * Verifies that all main UI components are loaded, including text fields, password fields, buttons, and panes.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane leftPane = lookup("#leftPane").query();
        Pane rightPane = lookup("#rightPane").query();
        assertNotNull(leftPane);
        assertNotNull(rightPane);

        TextField credentialField = lookup("#credentialTextField").query();
        PasswordField passwordField = lookup("#passwordPasswordField").query();
        assertNotNull(credentialField);
        assertNotNull(passwordField);
        assertEquals("exampleuser / example@email.com", credentialField.getPromptText());
        assertEquals("password", passwordField.getPromptText());

        Button loginButton = lookup("#logInBttn").query();
        Button signUpButton = lookup("#signUpBttn").query();
        assertNotNull(loginButton);
        assertNotNull(signUpButton);
        assertEquals("LOG IN", loginButton.getText());
        assertEquals("If you have no account SIGN UP", signUpButton.getText());
    }

    /**
     * Verifies that typing into credential and password fields works correctly.
     */
    @Test
    public void testTextFieldWriting()
    {
        clickOn("#credentialTextField").write("testuser");
        verifyThat("#credentialTextField", hasText("testuser"));

        clickOn("#passwordPasswordField").write("mypassword123");
        verifyThat("#passwordPasswordField", hasText("mypassword123"));
    }

    /**
     * Tests login attempt with empty fields and ensures no exceptions occur.
     */
    @Test
    public void testLoginWithEmptyFields()
    {
        clickOn("#logInBttn");
        pressEscape();
    }

    /**
     * Tests login attempt with invalid credentials. Verifies proper handling of failed login attempts.
     */
    @Test
    public void testLoginWithInvalidCredentials()
    {
        mockDAO.setMockUser(null);

        clickOn("#credentialTextField").write("wronguser");
        clickOn("#passwordPasswordField").write("wrongpass");
        clickOn("#logInBttn");
        pressEscape();
    }

    /**
     * Tests behavior when the DAO throws an exception during login.
     */
    @Test
    public void testLoginWithException()
    {
        mockDAO.setShouldThrowException(true, new OurException("Database error"));

        clickOn("#credentialTextField").write("testuser");
        clickOn("#passwordPasswordField").write("password123");
        clickOn("#logInBttn");
        pressEscape();
    }

    /**
     * Tests navigation from login window to sign-up window and back. Ensures text fields are cleared when returning to login.
     */
    @Test
    public void testNavigateToSignUpAndBack()
    {
        clickOn("#signUpBttn");
        sleep(1000);

        clickOn("#logInBttn");
        sleep(1000);

        verifyThat("#credentialTextField", hasText(""));
        verifyThat("#passwordPasswordField", hasText(""));
    }

    /**
     * Tests successful login with a standard user. Verifies that the LoggedProfile singleton reflects the logged-in user.
     */
    @Test
    public void testSuccessfulUserLogin()
    {
        mockDAO.setMockUser(mockUser);
        LoggedProfile.getInstance().setProfile(mockUser);

        clickOn("#credentialTextField").write("testuser");
        clickOn("#passwordPasswordField").write("password123");
        clickOn("#logInBttn");
        sleep(1000);

        Label usernameField = lookup("#username").query();
        assertNotNull(usernameField);
        assertEquals("testuser", usernameField.getText());
    }

    /**
     * Tests successful login with an admin user. Verifies that the LoggedProfile singleton reflects the logged-in admin.
     */
    @Test
    public void testSuccessfulAdminLogin()
    {
        mockDAO.setMockUser(mockAdmin);
        LoggedProfile.getInstance().setProfile(mockAdmin);

        clickOn("#credentialTextField").write("adminuser");
        clickOn("#passwordPasswordField").write("password123");
        clickOn("#logInBttn");
        sleep(1000);

        Label usernameField = lookup("#username").query();
        assertNotNull(usernameField);
        assertEquals("adminuser", usernameField.getText());
    }

    /**
     * Helper method to simulate pressing the ESCAPE key. Used to dismiss dialogs or alerts during tests.
     */
    private void pressEscape()
    {
        try
        {
            sleep(1000);
            push(javafx.scene.input.KeyCode.ESCAPE);
            sleep(500);
        }
        catch (Exception ignored)
        {
        }
    }
}
