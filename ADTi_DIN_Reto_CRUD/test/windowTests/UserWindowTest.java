package windowTests;

import controller.UserWindowController;
import controller.Controller;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
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
 * Represents a set of automated UI tests for the UserWindow JavaFX interface. This class extends ApplicationTest to provide a testing environment for JavaFX components using TestFX and JUnit. It ensures that the user interface components are properly loaded, user data is correctly displayed, and main actions such as saving changes, deleting a user, and logging out function as expected.
 *
 * MockModelDAO is used to simulate data access, and LoggedProfile singleton is used to manage the currently logged-in user during tests.
 */
public class UserWindowTest extends ApplicationTest
{

    /**
     * Controller instance for the UserWindow, used to manipulate and verify UI behavior during tests.
     */
    private UserWindowController userController;

    /**
     * Real controller instance that manages interactions with the data layer and provides application logic.
     */
    private Controller realController;

    /**
     * Mock DAO object to simulate database operations and control expected behavior or exceptions.
     */
    private MockModelDAO mockDAO;

    /**
     * Mock user instance used to populate the UI fields for testing.
     */
    private User mockUser;

    /**
     * Initializes the JavaFX stage for testing the UserWindow interface. Loads the FXML, sets the scene, initializes controllers, and sets a mock user in LoggedProfile to simulate a logged-in state.
     *
     * @param stage the JavaFX stage used to display the UserWindow
     * @throws Exception if loading the FXML fails or other initialization errors occur
     */
    @Override
    public void start(Stage stage) throws Exception
    {
        mockDAO = new MockModelDAO();
        realController = new Controller(mockDAO);

        mockUser = new User(
                1,
                "user@example.com",
                "testuser",
                "Password123",
                "Test",
                "User",
                "123456789",
                Gender.MALE,
                "1234567890123456"
        );
        LoggedProfile.getInstance().setProfile(mockUser);

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/UserWindow.fxml"));
        Parent root = loader.load();
        userController = loader.getController();

        if (userController != null)
        {
            userController.setController(realController);
        }

        stage.setScene(new Scene(root));
        stage.show();
    }

    /**
     * Prepares the testing environment before each test case. Clears the logged-in profile and resets the mock DAO behavior.
     */
    @Before
    public void setUp()
    {
        LoggedProfile.getInstance().clear();
        mockDAO.setShouldThrowException(false, null);
    }

    /**
     * Verifies that all main UI components are loaded correctly. Checks the existence of left and right panes, username label, and buttons for deleting user, saving changes, and logging out. Also verifies that button labels and username label contain the expected text.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane leftPane = lookup("#leftPane").query();
        Pane rightPane = lookup("#rightPane").query();
        Label usernameLabel = lookup("#username").query();
        Button deleteButton = lookup("#deleteUserBttn").query();
        Button saveButton = lookup("#saveChangesBttn").query();
        Button logoutButton = lookup("#logOutBttn").query();

        assertNotNull(leftPane);
        assertNotNull(rightPane);
        assertNotNull(usernameLabel);
        assertNotNull(deleteButton);
        assertNotNull(saveButton);
        assertNotNull(logoutButton);

        assertEquals("testuser", usernameLabel.getText());
        assertEquals("DELETE USER", deleteButton.getText());
        assertEquals("SAVE CHANGES", saveButton.getText());
        assertEquals("LOG OUT", logoutButton.getText());
    }

    /**
     * Verifies that user data is loaded correctly into the UI fields. Checks text fields for username, email, name, lastname, and telephone, password field, gender radio buttons, and card number fields.
     */
    @Test
    public void testUserDataLoadedCorrectly()
    {
        TextField usernameTextField = lookup("#usernameTextField").query();
        TextField emailTextField = lookup("#emailTextField").query();
        TextField nameTextField = lookup("#nameTextField").query();
        TextField lastnameTextField = lookup("#lastnameTextField").query();
        TextField telephoneTextField = lookup("#telephoneTextField").query();
        PasswordField passwordField = lookup("#passwordPasswordField").query();
        RadioButton maleRadioButton = lookup("#maleRadioButton").query();

        assertEquals("testuser", usernameTextField.getText());
        assertEquals("user@example.com", emailTextField.getText());
        assertEquals("Test", nameTextField.getText());
        assertEquals("User", lastnameTextField.getText());
        assertEquals("123456789", telephoneTextField.getText());
        assertEquals("Password123", passwordField.getText());
        assertTrue(maleRadioButton.isSelected());

        TextField card1 = lookup("#cardNumber1TextField").query();
        TextField card2 = lookup("#cardNumber2TextField").query();
        TextField card3 = lookup("#cardNumber3TextField").query();
        TextField card4 = lookup("#cardNumber4TextField").query();

        assertEquals("1234", card1.getText());
        assertEquals("5678", card2.getText());
        assertEquals("9012", card3.getText());
        assertEquals("3456", card4.getText());
    }

    /**
     * Tests the "Save Changes" button with valid user input. Modifies the name field and triggers the save action to verify that the UI interaction completes without exceptions.
     */
    @Test
    public void testSaveChangesWithValidData()
    {
        clickOn("#nameTextField").push(javafx.scene.input.KeyCode.CONTROL, javafx.scene.input.KeyCode.A);
        write("Jane");
        clickOn("#saveChangesBttn");
        sleep(500);
        pressEscape();
    }

    /**
     * Tests that clicking the "Delete User" button opens the verification window. Verifies that the verification window displays the correct title and closes it to prevent blocking other tests.
     */
    @Test
    public void testDeleteUserOpensVerifyWindow()
    {
        LoggedProfile.getInstance().setProfile(new User(
                1,
                "user@example.com",
                "testuser",
                "Password123",
                "Test",
                "User",
                "123456789",
                Gender.MALE,
                "1234567890123456"
        ));

        clickOn("#deleteUserBttn");
        sleep(500);

        Label titleLabel = lookup("#titleLabel").query();
        assertNotNull(titleLabel);
        assertEquals("ENTER YOUR PASSWORD", titleLabel.getText());

        Stage verifyStage = (Stage) titleLabel.getScene().getWindow();
        interact(verifyStage::close);
    }

    /**
     * Tests that clicking the "Log Out" button executes without error. This simulates a logout action in the UI and ensures no exceptions occur during interaction.
     */
    @Test
    public void testLogOutButton()
    {
        clickOn("#logOutBttn");
        sleep(500);
    }

    /**
     * Helper method to simulate pressing the ESCAPE key. Used to close popup dialogs or windows during tests.
     */
    private void pressEscape()
    {
        try
        {
            sleep(500);
            push(javafx.scene.input.KeyCode.ESCAPE);
            sleep(300);
        }
        catch (Exception ignored)
        {
        }
    }
}
