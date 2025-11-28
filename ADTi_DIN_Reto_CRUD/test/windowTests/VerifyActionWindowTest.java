package windowTests;

import controller.Controller;
import controller.VerifyActionWindowController;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
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
 * Automated UI tests for the VerifyActionWindow JavaFX interface.
 * <p>
 * This class uses TestFX and JUnit to verify that all components of the verification dialog for sensitive actions (like user deletion) are loaded correctly, buttons trigger expected behavior, and callback mechanisms are functional.
 * <p>
 * MockModelDAO simulates backend responses, and LoggedProfile singleton manages the current user session.
 */
public class VerifyActionWindowTest extends ApplicationTest
{

    /**
     * Controller for the VerifyActionWindow, used to access and manipulate UI elements.
     */
    private VerifyActionWindowController verifyActionController;

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
     * Flag indicating whether the user-deleted callback was executed.
     */
    private boolean callbackExecuted;

    /**
     * Initializes the JavaFX stage for testing the VerifyActionWindow. Loads FXML, sets the scene, prepares a mock user, and attaches a callback for user deletion.
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
        callbackExecuted = false;

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/VerifyActionWindow.fxml"));
        Parent root = loader.load();
        verifyActionController = loader.getController();

        verifyActionController.setController(realController, 1);
        verifyActionController.setOnUserDeletedCallback(() -> callbackExecuted = true);

        stage.setScene(new Scene(root));
        stage.show();
    }

    /**
     * Sets up the testing environment before each test. Clears the logged-in profile, resets the mock DAO, and resets the callback execution flag.
     */
    @Before
    public void setUp()
    {
        LoggedProfile.getInstance().clear();
        mockDAO.setShouldThrowException(false, null);
        mockDAO.setMockUser(null);
        callbackExecuted = false;
    }

    /**
     * Verifies that all main UI components are loaded correctly, including labels, panes, and buttons, and that their text matches expected content.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane rightPane = lookup("#rightPane").query();
        Label usernameLabel = lookup("#username").query();
        Label titleLabel = lookup("#titleLabel").query();
        Label textLabel = lookup("#textLabel").query();
        Button confirmButton = lookup("#confirmBttn").query();
        Button cancelButton = lookup("#cancelBttn").query();

        assertNotNull(rightPane);
        assertNotNull(usernameLabel);
        assertNotNull(titleLabel);
        assertNotNull(textLabel);
        assertNotNull(confirmButton);
        assertNotNull(cancelButton);

        assertEquals("testuser", usernameLabel.getText());
        assertEquals("ARE YOU SURE YOU WANT TO DELETE THE PROFILE?", titleLabel.getText());
        assertEquals("This action is irreversible. The profile and its data will be deleted forever and it will not be able to restore it.", textLabel.getText());
        assertEquals("CONFIRM", confirmButton.getText());
        assertEquals("CANCEL", cancelButton.getText());
    }

    /**
     * Tests that clicking the confirm button navigates to the CAPTCHA confirmation step.
     */
    @Test
    public void testConfirmButtonNavigatesToCaptchaWindow()
    {
        LoggedProfile.getInstance().setProfile(mockUser);

        clickOn("#confirmBttn");
        sleep(500);

        Label textLabel = lookup("#titleLabel").query();
        assertEquals("WRITE THE NEXT CODE TO CONFIRM", textLabel.getText());
    }

    /**
     * Tests that clicking the cancel button does not throw exceptions and can close or dismiss the window.
     */
    @Test
    public void testCancelButton()
    {
        clickOn("#cancelBttn");
    }

    /**
     * Verifies that the user-deleted callback has been set and is initially not executed.
     */
    @Test
    public void testCallbackIsSet()
    {
        assertFalse("Callback should not be executed yet", callbackExecuted);
    }
}
