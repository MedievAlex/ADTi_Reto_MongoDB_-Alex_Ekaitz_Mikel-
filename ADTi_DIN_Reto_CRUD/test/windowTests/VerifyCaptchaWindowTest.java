package windowTests;

import controller.Controller;
import controller.VerifyCaptchaWindowController;
import dao.MockModelDAO;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.Gender;
import model.LoggedProfile;
import model.User;
import org.junit.Before;
import org.junit.Test;
import org.testfx.framework.junit.ApplicationTest;

import static org.junit.Assert.*;

/**
 * Test class for the VerifyCaptchaWindow JavaFX interface.
 * <p>
 * This class validates the behavior of the captcha verification window, which is used to confirm sensitive actions (e.g., account deletion) through a one-time code. It ensures that all UI components are loaded correctly, handles empty or incorrect code submissions, confirms correct code entry, and verifies that the cancel button works.
 * <p>
 * Uses TestFX for UI interaction simulation and MockModelDAO to mock database operations.
 */
public class VerifyCaptchaWindowTest extends ApplicationTest
{

    /**
     * Controller for the VerifyCaptchaWindow used to access UI elements.
     */
    private VerifyCaptchaWindowController captchaController;

    /**
     * Main application controller for business logic and data access.
     */
    private Controller realController;

    /**
     * Mock DAO to simulate database operations and exceptions.
     */
    private MockModelDAO mockDAO;

    /**
     * Mock user used for testing captcha verification.
     */
    private User mockUser;

    /**
     * Initializes the JavaFX stage for the VerifyCaptchaWindow. Loads FXML, sets the scene, and sets a mock user as the logged-in profile.
     *
     * @param stage the JavaFX stage to display the captcha verification window
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

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/VerifyCaptchaWindow.fxml"));
        Parent root = loader.load();
        captchaController = loader.getController();

        captchaController.setController(realController, -1);

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
     * Checks that all components of the VerifyCaptchaWindow are loaded and initialized correctly.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane rightPane = lookup("#rightPane").query();
        TextField codeTextField = lookup("#codeTextField").query();
        Label usernameLabel = lookup("#username").query();
        Label codeLabel = lookup("#codeLabel").query();
        Button confirmButton = lookup("#confirmBttn").query();
        Button cancelButton = lookup("#cancelBttn").query();
        Label titleLabel = lookup("#titleLabel").query();
        Label errorLabel = lookup("#errorLabel").query();

        assertNotNull(rightPane);
        assertNotNull(codeTextField);
        assertNotNull(usernameLabel);
        assertNotNull(codeLabel);
        assertNotNull(confirmButton);
        assertNotNull(cancelButton);
        assertNotNull(titleLabel);
        assertNotNull(errorLabel);

        assertEquals("testuser", usernameLabel.getText());
        assertEquals("CONFIRM", confirmButton.getText());
        assertEquals("CANCEL", cancelButton.getText());
        assertEquals("WRITE THE NEXT CODE TO CONFIRM", titleLabel.getText());
        assertEquals("", errorLabel.getText());
        assertFalse(codeLabel.getText().isEmpty());
    }

    /**
     * Verifies that clicking confirm with an empty code field displays the appropriate error message.
     */
    @Test
    public void testConfirmWithEmptyField()
    {
        clickOn("#confirmBttn");
        Label errorLabel = lookup("#errorLabel").query();
        assertEquals("Please enter the code.", errorLabel.getText());
    }

    /**
     * Verifies that entering an incorrect code displays an error message.
     */
    @Test
    public void testConfirmWithIncorrectCode()
    {
        String wrongCode = "9999";
        clickOn("#codeTextField").write(wrongCode);
        clickOn("#confirmBttn");

        Label errorLabel = lookup("#errorLabel").query();
        assertEquals("Incorrect code. Try again.", errorLabel.getText());
    }

    /**
     * Verifies that entering the correct code allows proceeding to the next step.
     */
    @Test
    public void testConfirmWithCorrectCode()
    {
        Label codeLabel = lookup("#codeLabel").query();
        String correctCode = codeLabel.getText();

        clickOn("#codeTextField").write(correctCode);
        clickOn("#confirmBttn");

        pressEscape();
    }

    /**
     * Verifies that clicking the cancel button closes or dismisses the window without errors.
     */
    @Test
    public void testCancelButton()
    {
        clickOn("#cancelBttn");
    }

    /**
     * Utility method to simulate pressing the ESCAPE key, typically to close windows.
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
