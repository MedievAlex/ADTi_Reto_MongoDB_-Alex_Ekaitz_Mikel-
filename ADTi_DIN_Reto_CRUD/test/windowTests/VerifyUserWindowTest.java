package windowTests;

import controller.Controller;
import controller.VerifyUserWindowController;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.LoggedProfile;
import model.User;
import model.Gender;
import org.junit.Before;
import org.junit.Test;
import org.testfx.framework.junit.ApplicationTest;
import dao.MockModelDAO;

import static org.junit.Assert.*;

/**
 * Test class for the VerifyUserWindow JavaFX interface.
 * <p>
 * This class verifies the behavior of the user verification window where users must confirm their password before performing sensitive actions, such as account deletion. It checks that UI components are loaded, password verification works, and the cancel button functions correctly.
 * <p>
 * Uses TestFX for JavaFX UI testing and a MockModelDAO to simulate database operations.
 */
public class VerifyUserWindowTest extends ApplicationTest
{

    /**
     * Controller for the VerifyUserWindow, used to access and manipulate UI elements.
     */
    private VerifyUserWindowController verifyController;

    /**
     * Main controller handling application logic and data interactions.
     */
    private Controller realController;

    /**
     * Mock DAO to simulate database operations and exceptions.
     */
    private MockModelDAO mockDAO;

    /**
     * Mock user used for testing verification actions.
     */
    private User mockUser;

    /**
     * Initializes the JavaFX stage for testing the VerifyUserWindow. Loads FXML, sets the scene, and sets a mock user as the logged-in profile.
     *
     * @param stage the JavaFX stage to display the verification window
     * @throws Exception if FXML loading fails
     */
    @Override
    public void start(Stage stage) throws Exception
    {
        mockDAO = new MockModelDAO();
        realController = new Controller(mockDAO);

        mockUser = new User(1, "test@example.com", "testuser", "Password123",
                "John", "Doe", "123456789", Gender.MALE, "1234567890123456");

        LoggedProfile.getInstance().setProfile(mockUser);

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/VerifyUserWindow.fxml"));
        Parent root = loader.load();
        verifyController = loader.getController();

        verifyController.setController(realController, -1);

        stage.setScene(new Scene(root));
        stage.show();
    }

    /**
     * Sets up the testing environment before each test. Clears the logged-in profile and resets the mock DAO.
     */
    @Before
    public void setUp()
    {
        LoggedProfile.getInstance().clear();
        mockDAO.setShouldThrowException(false, null);
        mockDAO.setMockUser(null);
    }

    /**
     * Verifies that all UI components are loaded and contain correct default values.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane rightPane = lookup("#rightPane").query();
        PasswordField passwordField = lookup("#passwordPasswordField").query();
        Label usernameLabel = lookup("#username").query();
        Button confirmButton = lookup("#confirmBttn").query();
        Button cancelButton = lookup("#cancelBttn").query();
        Label titleLabel = lookup("#titleLabel").query();
        Label errorLabel = lookup("#errorLabel").query();

        assertNotNull(rightPane);
        assertNotNull(passwordField);
        assertNotNull(usernameLabel);
        assertNotNull(confirmButton);
        assertNotNull(cancelButton);
        assertNotNull(titleLabel);
        assertNotNull(errorLabel);

        assertEquals("testuser", usernameLabel.getText());
        assertEquals("CONFIRM", confirmButton.getText());
        assertEquals("CANCEL", cancelButton.getText());
        assertEquals("ENTER YOUR PASSWORD", titleLabel.getText());
        assertEquals("· · · · · · · ·", passwordField.getPromptText());
        assertEquals("", errorLabel.getText());
    }

    /**
     * Tests that entering an incorrect password displays the appropriate error message.
     */
    @Test
    public void testConfirmWithIncorrectPassword()
    {
        clickOn("#passwordPasswordField").write("WrongPassword");
        clickOn("#confirmBttn");

        Label errorLabel = lookup("#errorLabel").query();
        assertEquals("Incorrect password.", errorLabel.getText());
    }

    /**
     * Tests that entering the correct password allows navigation to the next verification step.
     */
    @Test
    public void testConfirmWithCorrectPassword()
    {
        LoggedProfile.getInstance().setProfile(mockUser);

        clickOn("#passwordPasswordField").write("Password123");
        clickOn("#confirmBttn");
        sleep(500);

        Label textLabel = lookup("#titleLabel").query();
        assertEquals("ARE YOU SURE YOU WANT TO DELETE THE PROFILE?", textLabel.getText());
    }

    /**
     * Verifies that clicking the cancel button closes or dismisses the window without errors.
     */
    @Test
    public void testCancelButton()
    {
        clickOn("#cancelBttn");
    }
}
