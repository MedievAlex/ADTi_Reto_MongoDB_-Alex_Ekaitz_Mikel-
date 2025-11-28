package windowTests;

import controller.AdminWindowController;
import controller.Controller;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.Admin;
import model.LoggedProfile;
import model.User;
import org.junit.Before;
import org.junit.Test;
import org.testfx.framework.junit.ApplicationTest;
import dao.MockModelDAO;

import static org.junit.Assert.*;

/**
 * Represents a set of automated UI tests for the AdminWindow JavaFX interface. This class extends ApplicationTest to provide a testing environment for JavaFX components using TestFX and JUnit. It ensures that the admin interface loads correctly, users are displayed, and main actions like saving changes, deleting users, and logging out function as expected.
 *
 * MockModelDAO is used to simulate database operations, and LoggedProfile singleton is used to manage the currently logged-in admin during tests.
 */
public class AdminWindowTest extends ApplicationTest
{

    /**
     * Controller instance for the AdminWindow, used to manipulate and verify UI behavior during tests.
     */
    private AdminWindowController adminController;

    /**
     * Real controller instance that manages interactions with the data layer and provides application logic.
     */
    private Controller realController;

    /**
     * Mock DAO object to simulate database operations and control expected behavior or exceptions.
     */
    private MockModelDAO mockDAO;

    /**
     * Mock admin instance used to populate the UI fields for testing.
     */
    private Admin mockAdmin;

    /**
     * Initializes the JavaFX stage for testing the AdminWindow interface. Loads the FXML, sets the scene, initializes controllers, and sets a mock admin in LoggedProfile to simulate a logged-in state.
     *
     * @param stage the JavaFX stage used to display the AdminWindow
     * @throws Exception if loading the FXML fails or other initialization errors occur
     */
    @Override
    public void start(Stage stage) throws Exception
    {
        mockDAO = new MockModelDAO();
        realController = new Controller(mockDAO);

        mockAdmin = new Admin(1, "admin@example.com", "admin", "Admin123", "Admin", "User", "123456789", "1234567890123456");
        LoggedProfile.getInstance().setProfile(mockAdmin);

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/AdminWindow.fxml"));
        Parent root = loader.load();
        adminController = loader.getController();

        if (adminController != null)
        {
            adminController.setController(realController);
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
     * Verifies that all main UI components are loaded correctly. Checks the existence of left and right panes, username label, ComboBox for users, and buttons for deleting, saving changes, and logging out. Also verifies button labels and username label content.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane leftPane = lookup("#leftPane").query();
        Pane rightPane = lookup("#rightPane").query();
        Label username = lookup("#username").query();
        ComboBox usersComboBox = lookup("#usersComboBox").query();
        Button deleteButton = lookup("#deleteUserBttn").query();
        Button saveButton = lookup("#saveChangesBttn").query();
        Button logoutButton = lookup("#logOutBttn").query();

        assertNotNull(leftPane);
        assertNotNull(rightPane);
        assertNotNull(username);
        assertNotNull(usersComboBox);
        assertNotNull(deleteButton);
        assertNotNull(saveButton);
        assertNotNull(logoutButton);

        assertEquals("admin", username.getText());
        assertEquals("DELETE USER", deleteButton.getText());
        assertEquals("SAVE CHANGES", saveButton.getText());
        assertEquals("LOG OUT", logoutButton.getText());
    }

    /**
     * Verifies that the username label is correctly set from the logged-in profile.
     */
    @Test
    public void testUsernameIsSetFromLoggedProfile()
    {
        Label username = lookup("#username").query();
        assertEquals("admin", username.getText());
    }

    /**
     * Verifies that the users ComboBox is populated with users.
     */
    @Test
    public void testUsersComboBoxIsPopulated()
    {
        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        assertFalse(usersComboBox.getItems().isEmpty());
    }

    /**
     * Tests selecting a user from the ComboBox and ensures the corresponding fields are loaded correctly.
     */
    @Test
    public void testSelectUserFromComboBox()
    {
        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        sleep(500);
        interact(() -> usersComboBox.getSelectionModel().select(0));
        sleep(300);

        TextField usernameTextField = lookup("#usernameTextField").query();
        assertFalse(usernameTextField.getText().isEmpty());
    }

    /**
     * Tests the behavior of saving changes without selecting a user. Ensures that the action completes without exceptions.
     */
    @Test
    public void testSaveChangesWithoutSelectedUser()
    {
        clickOn("#saveChangesBttn");
        sleep(500);
        pressEscape();
    }

    /**
     * Tests the behavior of deleting a user without selecting a user. Ensures that the action completes without exceptions.
     */
    @Test
    public void testDeleteUserWithoutSelectedUser()
    {
        clickOn("#deleteUserBttn");
        sleep(500);
        pressEscape();
    }

    /**
     * Tests that clicking the "Log Out" button executes correctly without throwing exceptions.
     */
    @Test
    public void testLogOutButton()
    {
        clickOn("#logOutBttn");
        sleep(500);
    }

    /**
     * Verifies that the user data fields are loaded correctly when selecting a user from the ComboBox.
     */
    @Test
    public void testUserDataLoadedCorrectly()
    {
        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        sleep(500);

        interact(() -> usersComboBox.getSelectionModel().select(0));
        sleep(500);

        TextField usernameTextField = lookup("#usernameTextField").query();
        TextField emailTextField = lookup("#emailTextField").query();
        TextField nameTextField = lookup("#nameTextField").query();
        TextField lastnameTextField = lookup("#lastnameTextField").query();
        TextField telephoneTextField = lookup("#telephoneTextField").query();
        PasswordField passwordField = lookup("#passwordPasswordField").query();

        assertEquals("testuser", usernameTextField.getText());
        assertEquals("test@test.com", emailTextField.getText());
        assertEquals("Test", nameTextField.getText());
        assertEquals("User", lastnameTextField.getText());
        assertEquals("123456789", telephoneTextField.getText());
        assertEquals("Ab123456", passwordField.getText());
    }

    /**
     * Verifies that the gender radio buttons reflect the user's gender correctly.
     */
    @Test
    public void testGenderRadioButtons()
    {
        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        sleep(500);

        interact(() -> usersComboBox.getSelectionModel().select(0));
        sleep(500);

        RadioButton maleRadioButton = lookup("#maleRadioButton").query();
        assertTrue(maleRadioButton.isSelected());
    }

    /**
     * Verifies that the card number fields are populated correctly for the selected user.
     */
    @Test
    public void testCardNumberFields()
    {
        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        sleep(500);

        interact(() -> usersComboBox.getSelectionModel().select(0));
        sleep(500);

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
     * Tests saving changes after selecting a user and modifying fields.
     */
    @Test
    public void testSaveChangesWithSelectedUser()
    {
        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        sleep(500);

        interact(() -> usersComboBox.getSelectionModel().select(0));
        sleep(500);

        TextField nameTextField = lookup("#nameTextField").query();
        clickOn("#nameTextField").push(javafx.scene.input.KeyCode.CONTROL, javafx.scene.input.KeyCode.A);
        write("Jane");

        clickOn("#saveChangesBttn");
        sleep(500);
        pressEscape();
    }

    /**
     * Tests that deleting a selected user opens the verification window and the window title is displayed correctly.
     */
    @Test
    public void testDeleteUserWithSelectedUserShowsTitle()
    {
        LoggedProfile.getInstance().setProfile(new Admin(
                1, "admin@example.com", "admin", "Admin123",
                "Admin", "User", "123456789", "1234567890123456"
        ));

        ComboBox<User> usersComboBox = lookup("#usersComboBox").query();
        interact(() -> usersComboBox.getSelectionModel().select(0));
        sleep(500);

        clickOn("#deleteUserBttn");
        sleep(500);

        Label titleLabel = lookup("#titleLabel").query();
        assertNotNull(titleLabel);
        assertEquals("ENTER YOUR PASSWORD", titleLabel.getText());

        Stage verifyStage = (Stage) titleLabel.getScene().getWindow();
        interact(verifyStage::close);
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
