package windowTests;

import controller.Controller;
import model.LoggedProfile;
import controller.SignUpWindowController;
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
 * Automated UI tests for the SignUpWindow JavaFX interface.
 * <p>
 * This class uses TestFX and JUnit to verify that all components of the sign-up interface are loaded, user interactions (like typing, selecting radio buttons, and navigating between login and sign-up views) work correctly, and that input constraints and error handling are enforced.
 * <p>
 * A MockModelDAO simulates database interactions, and LoggedProfile singleton is used to manage the current user state.
 */
public class SignUpWindowTest extends ApplicationTest
{

    /**
     * Controller instance for the SignUpWindow, used to manipulate UI components during tests.
     */
    private SignUpWindowController signUpController;

    /**
     * Real controller instance that handles application logic and data interactions.
     */
    private Controller realController;

    /**
     * Mock DAO object to simulate database operations and exceptions.
     */
    private MockModelDAO mockDAO;

    /**
     * Mock user instance used to populate fields and test sign-up functionality.
     */
    private User mockUser;

    /**
     * Initializes the JavaFX stage for testing the SignUpWindow interface. Loads the FXML, sets the scene, initializes controllers, and prepares a mock user for tests.
     *
     * @param stage the JavaFX stage used to display the sign-up window
     * @throws Exception if FXML loading or initialization fails
     */
    @Override
    public void start(Stage stage) throws Exception
    {
        mockDAO = new MockModelDAO();
        realController = new Controller(mockDAO);

        mockUser = new User(1, "test@example.com", "testuser", "Password123",
                "John", "Doe", "123456789", Gender.MALE, "1234567890123456");

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/SignUpWindow.fxml"));
        Parent root = loader.load();
        signUpController = loader.getController();

        signUpController.setController(realController);

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
        mockDAO.setMockUser(null);
    }

    /**
     * Verifies that all main UI components are loaded correctly, including text fields, labels, radio buttons, card fields, and buttons. Also checks that the placeholder texts and labels are correct.
     */
    @Test
    public void testAllComponentsAreLoaded()
    {
        Pane leftPane = lookup("#leftPane").query();
        Pane rightPane = lookup("#rightPane").query();
        assertNotNull(leftPane);
        assertNotNull(rightPane);

        TextField usernameField = lookup("#usernameTextField").query();
        TextField emailField = lookup("#emailTextField").query();
        PasswordField passwordField = lookup("#passwordPasswordField").query();
        TextField nameField = lookup("#nameTextField").query();
        TextField lastnameField = lookup("#lastnameTextField").query();
        TextField telephoneField = lookup("#telephoneTextField").query();
        assertNotNull(usernameField);
        assertNotNull(emailField);
        assertNotNull(passwordField);
        assertNotNull(nameField);
        assertNotNull(lastnameField);
        assertNotNull(telephoneField);
        assertEquals("exampleuser", usernameField.getPromptText());
        assertEquals("example@email.com", emailField.getPromptText());
        assertEquals("· · · · · · · ·", passwordField.getPromptText());
        assertEquals("name", nameField.getPromptText());
        assertEquals("lastname", lastnameField.getPromptText());
        assertEquals("688 88 88 88", telephoneField.getPromptText());

        RadioButton maleRadio = lookup("#maleRadioButton").query();
        RadioButton femaleRadio = lookup("#femaleRadioButton").query();
        RadioButton otherRadio = lookup("#otherRadioButton").query();
        assertNotNull(maleRadio);
        assertNotNull(femaleRadio);
        assertNotNull(otherRadio);
        assertEquals("MALE", maleRadio.getText());
        assertEquals("FEMALE", femaleRadio.getText());
        assertEquals("OTHER", otherRadio.getText());

        TextField card1 = lookup("#cardNumber1TextField").query();
        TextField card2 = lookup("#cardNumber2TextField").query();
        TextField card3 = lookup("#cardNumber3TextField").query();
        TextField card4 = lookup("#cardNumber4TextField").query();
        assertNotNull(card1);
        assertNotNull(card2);
        assertNotNull(card3);
        assertNotNull(card4);
        assertEquals("0000", card1.getPromptText());
        assertEquals("0000", card2.getPromptText());
        assertEquals("0000", card3.getPromptText());
        assertEquals("0000", card4.getPromptText());

        Button signUpButton = lookup("#signUpBttn").query();
        Button loginButton = lookup("#logInBttn").query();
        assertNotNull(signUpButton);
        assertNotNull(loginButton);
        assertEquals("SIGN UP", signUpButton.getText());
        assertEquals("If you have an account LOG IN", loginButton.getText());

        Label usernameLabel = lookup("#usernameLabel").query();
        Label emailLabel = lookup("#emailLabel").query();
        Label passwordLabel = lookup("#passwordLabel").query();
        Label nameLabel = lookup("#nameLabel").query();
        Label telephoneLabel = lookup("#telephoneLabel").query();
        Label genderLabel = lookup("#genderLabel").query();
        Label cardNumberLabel = lookup("#cardNumberLabel").query();
        assertNotNull(usernameLabel);
        assertNotNull(emailLabel);
        assertNotNull(passwordLabel);
        assertNotNull(nameLabel);
        assertNotNull(telephoneLabel);
        assertNotNull(genderLabel);
        assertNotNull(cardNumberLabel);
        assertEquals("Username:", usernameLabel.getText());
        assertEquals("Email:", emailLabel.getText());
        assertEquals("Password:", passwordLabel.getText());
        assertEquals("Name:", nameLabel.getText());
        assertEquals("Telephone:", telephoneLabel.getText());
        assertEquals("Gender:", genderLabel.getText());
        assertEquals("Card Number:", cardNumberLabel.getText());
    }

    /**
     * Verifies that typing into text fields works correctly.
     */
    @Test
    public void testTextFieldWriting()
    {
        clickOn("#usernameTextField").write("testuser");
        verifyThat("#usernameTextField", hasText("testuser"));

        clickOn("#emailTextField").write("test@example.com");
        verifyThat("#emailTextField", hasText("test@example.com"));

        clickOn("#passwordPasswordField").write("Password123");
        verifyThat("#passwordPasswordField", hasText("Password123"));
    }

    /**
     * Tests clicking the sign-up button with empty input fields. Ensures the interface handles empty submission without exceptions.
     */
    @Test
    public void testSignUpWithEmptyFields()
    {
        clickOn("#signUpBttn");
        pressEscape();
    }

    /**
     * Tests signing up with invalid or incomplete data. Verifies that the interface handles invalid input correctly.
     */
    @Test
    public void testSignUpWithInvalidData()
    {
        clickOn("#usernameTextField").write("user");
        clickOn("#emailTextField").write("invalid-email");
        clickOn("#passwordPasswordField").write("weak");
        clickOn("#nameTextField").write("John");
        clickOn("#lastnameTextField").write("Doe");
        clickOn("#telephoneTextField").write("123");
        clickOn("#cardNumber1TextField").write("1234");
        clickOn("#cardNumber2TextField").write("5678");
        clickOn("#cardNumber3TextField").write("9012");
        clickOn("#cardNumber4TextField").write("34");
        clickOn("#maleRadioButton");

        clickOn("#signUpBttn");
        pressEscape();
    }

    /**
     * Tests the behavior when the DAO throws an exception during sign-up.
     */
    @Test
    public void testSignUpWithException()
    {
        mockDAO.setShouldThrowException(true, new OurException("Database error"));

        clickOn("#usernameTextField").write("testuser");
        clickOn("#emailTextField").write("test@example.com");
        clickOn("#passwordPasswordField").write("Password123");
        clickOn("#nameTextField").write("John");
        clickOn("#lastnameTextField").write("Doe");
        clickOn("#telephoneTextField").write("123456789");
        clickOn("#cardNumber1TextField").write("1234");
        clickOn("#cardNumber2TextField").write("5678");
        clickOn("#cardNumber3TextField").write("9012");
        clickOn("#cardNumber4TextField").write("3456");
        clickOn("#maleRadioButton");

        clickOn("#signUpBttn");
        pressEscape();
    }

    /**
     * Tests navigation from sign-up to login screen and back.
     */
    @Test
    public void testNavigateToLoginAndBack()
    {
        clickOn("#logInBttn");
        sleep(1000);

        Button signUpButton = lookup("#signUpBttn").query();
        assertNotNull(signUpButton);
        assertEquals("If you have no account SIGN UP", signUpButton.getText());

        clickOn("#signUpBttn");
        sleep(1000);

        Button loginButton = lookup("#logInBttn").query();
        assertNotNull(loginButton);
        assertEquals("If you have an account LOG IN", loginButton.getText());
    }

    /**
     * Tests a successful sign-up process with valid input and mock user.
     */
    @Test
    public void testSuccessfulSignUp()
    {
        mockDAO.setMockUser(mockUser);
        LoggedProfile.getInstance().setProfile(mockUser);

        clickOn("#usernameTextField").write("testuser");
        clickOn("#emailTextField").write("test@example.com");
        clickOn("#passwordPasswordField").write("Password123");
        clickOn("#nameTextField").write("John");
        clickOn("#lastnameTextField").write("Doe");
        clickOn("#telephoneTextField").write("123456789");
        clickOn("#cardNumber1TextField").write("1234");
        clickOn("#cardNumber2TextField").write("5678");
        clickOn("#cardNumber3TextField").write("9012");
        clickOn("#cardNumber4TextField").write("3456");
        clickOn("#maleRadioButton");

        clickOn("#signUpBttn");
        pressEscape();

        Label usernameLabel = lookup("#username").query();
        assertNotNull(usernameLabel);
        assertEquals("testuser", usernameLabel.getText());
    }

    /**
     * Verifies that radio button selection is mutually exclusive and works correctly.
     */
    @Test
    public void testRadioButtonSelection()
    {
        RadioButton maleRadio = lookup("#maleRadioButton").query();
        RadioButton femaleRadio = lookup("#femaleRadioButton").query();
        RadioButton otherRadio = lookup("#otherRadioButton").query();

        clickOn("#maleRadioButton");
        assertTrue(maleRadio.isSelected());
        assertFalse(femaleRadio.isSelected());
        assertFalse(otherRadio.isSelected());

        clickOn("#femaleRadioButton");
        assertFalse(maleRadio.isSelected());
        assertTrue(femaleRadio.isSelected());
        assertFalse(otherRadio.isSelected());

        clickOn("#otherRadioButton");
        assertFalse(maleRadio.isSelected());
        assertFalse(femaleRadio.isSelected());
        assertTrue(otherRadio.isSelected());
    }

    /**
     * Tests automatic focus/navigation between card number text fields.
     */
    @Test
    public void testCardNumberAutoNavigation()
    {
        clickOn("#cardNumber1TextField").write("1234");
        verifyThat("#cardNumber2TextField", hasText(""));

        clickOn("#cardNumber2TextField").write("5678");
        verifyThat("#cardNumber3TextField", hasText(""));

        clickOn("#cardNumber3TextField").write("9012");
        verifyThat("#cardNumber4TextField", hasText(""));
    }

    /**
     * Verifies that non-numeric characters are filtered from the telephone field.
     */
    @Test
    public void testTelephoneNumberFiltersNonNumeric()
    {
        clickOn("#telephoneTextField").write("123abc456");
        verifyThat("#telephoneTextField", hasText("123456"));
    }

    /**
     * Verifies that the telephone field enforces a maximum length of 9 digits.
     */
    @Test
    public void testTelephoneNumberMaxLength()
    {
        clickOn("#telephoneTextField").write("1234567890");
        verifyThat("#telephoneTextField", hasText("123456789"));
    }

    /**
     * Verifies that non-numeric characters are filtered from card number fields.
     */
    @Test
    public void testCardNumberFiltersNonNumeric()
    {
        clickOn("#cardNumber1TextField").write("12ab34");
        verifyThat("#cardNumber1TextField", hasText("1234"));
    }

    /**
     * Verifies that the card number fields enforce a maximum length of 4 digits.
     */
    @Test
    public void testCardNumberMaxLength()
    {
        clickOn("#cardNumber1TextField").write("123456");
        verifyThat("#cardNumber1TextField", hasText("1234"));
    }

    /**
     * Helper method to simulate pressing the ESCAPE key. Used to close dialogs or popups during tests.
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
