package controller;

import exception.OurException;
import exception.ShowAlert;
import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.Gender;
import model.Profile;
import model.User;

/**
 * Controller class for the User Registration Window interface. This class handles the complete user registration process, including form validation, data collection, and user account creation. It provides a comprehensive interface for new users to create accounts with proper validation for all required fields.
 *
 * The controller implements JavaFX Initializable interface to properly initialize the UI components and set up input validation handlers.
 */
public class SignUpWindowController implements Initializable
{

    private Controller controller;

    @FXML
    private Pane leftPane;
    @FXML
    private Label usernameLabel;
    @FXML
    private Label passwordLabel;
    @FXML
    private Label nameLabel;
    @FXML
    private Label telephoneLabel;
    @FXML
    private Label genderLabel;
    @FXML
    private Label emailLabel;
    @FXML
    private Label cardNumberLabel;
    @FXML
    private Pane rightPane;
    @FXML
    private TextField usernameTextField;
    @FXML
    private TextField emailTextField;
    @FXML
    private PasswordField passwordPasswordField;
    @FXML
    private TextField nameTextField;
    @FXML
    private TextField lastnameTextField;
    @FXML
    private TextField telephoneTextField;
    @FXML
    private RadioButton maleRadioButton;
    @FXML
    private RadioButton femaleRadioButton;
    @FXML
    private RadioButton otherRadioButton;
    @FXML
    private TextField cardNumber1TextField;
    @FXML
    private TextField cardNumber2TextField;
    @FXML
    private TextField cardNumber3TextField;
    @FXML
    private TextField cardNumber4TextField;
    @FXML
    private Button signUpBttn;
    @FXML
    private Button logInBttn;

    private final String ERROR_STYLE = "-fx-border-color: red; -fx-border-width: 2px;";
    private final String NORMAL_STYLE = "-fx-border-color: null;";

    /**
     * Sets the main controller for this registration window controller. This method establishes the connection to the main application controller that handles business logic and user registration operations.
     *
     * @param controller the main application controller that manages business logic and data operations
     */
    public void setController(Controller controller)
    {
        this.controller = controller;
    }

    /**
     * Handles the user registration process when the sign up button is clicked. This method validates all input fields, collects user data, creates a new User object, and attempts to register the user through the main controller. Upon successful registration, the user is automatically logged in and redirected to the user window.
     *
     * The method performs comprehensive validation including field completeness checks, email format validation, telephone number format validation, password strength requirements, and credit card number validation.
     *
     */
    @FXML
    public void handleSignUp()
    {
        if (!validateFields())
        {
            ShowAlert.showAlert("Validation Error",
                    "Please fill all required fields correctly:\n\n"
                    + "- Telephone must be exactly 9 digits\n"
                    + "- Password must be at least 8 characters with uppercase, lowercase and numbers\n"
                    + "- Card must be exactly 16 digits",
                    Alert.AlertType.ERROR);
            return;
        }

        String username = usernameTextField.getText().trim();
        String email = emailTextField.getText().trim();
        String password = passwordPasswordField.getText().trim();
        String name = nameTextField.getText().trim();
        String lastname = lastnameTextField.getText().trim();
        String telephone = telephoneTextField.getText().trim();

        Gender gender = Gender.OTHER;
        if (maleRadioButton.isSelected())
        {
            gender = Gender.MALE;
        }
        else if (femaleRadioButton.isSelected())
        {
            gender = Gender.FEMALE;
        }

        String cardNumber = cardNumber1TextField.getText()
                + cardNumber2TextField.getText()
                + cardNumber3TextField.getText()
                + cardNumber4TextField.getText();

        try
        {
            User user = new User(email, username, password, name, lastname, telephone, gender, cardNumber);

            User registeredUser = controller.register(user);

            if (registeredUser != null)
            {
                Profile loggedIn = controller.login(username, password);

                if (loggedIn != null)
                {
                    String fxmlPath = "/view/UserWindow.fxml";
                    FXMLLoader loader = new FXMLLoader(getClass().getResource(fxmlPath));
                    Parent window = loader.load();

                    UserWindowController userController = loader.getController();
                    userController.setController(this.controller);

                    Stage currentWindow = (Stage) signUpBttn.getScene().getWindow();
                    currentWindow.setTitle("User");
                    currentWindow.setScene(new Scene(window));

                    ShowAlert.showAlert("Success", "Account created successfully!", Alert.AlertType.INFORMATION);
                    resetFieldStyles();
                }
                else
                {
                    ShowAlert.showAlert("Error", "Account created but login failed.", Alert.AlertType.ERROR);
                }
            }
            else
            {
                ShowAlert.showAlert("Error", "User registration failed. Please try again.", Alert.AlertType.ERROR);
            }

        }
        catch (OurException ex)
        {
            ShowAlert.showAlert("Error", ex.getMessage(), Alert.AlertType.ERROR);
        }
        catch (IOException ex)
        {
            ShowAlert.showAlert("Error", "Error opening User window.", Alert.AlertType.ERROR);
        }
    }

    /**
     * Navigates back to the login window. This method loads the login interface and transitions the current window to display the login screen, typically used when the user chooses to return to login.
     *
     */
    @FXML
    public void openLogin()
    {
        try
        {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/LoginWindow.fxml"));
            Parent parentWindow = loader.load();
            LoginWindowController nextController = loader.getController();
            nextController.setController(controller);

            Stage actualWindow = (Stage) logInBttn.getScene().getWindow();
            actualWindow.setTitle("Logi In");
            actualWindow.setResizable(false);
            actualWindow.setScene(new Scene(parentWindow));
        }
        catch (IOException ex)
        {
            ShowAlert.showAlert("Error", "Error opening Login window.", Alert.AlertType.ERROR);
        }
    }

    /**
     * Validates all input fields in the registration form. This method checks each required field for proper formatting, completeness, and content according to business rules, applying visual error styling to invalid fields.
     *
     * @return true if all fields pass validation, false if any validation rule is violated
     */
    private boolean validateFields()
    {
        boolean isValid = true;
        resetFieldStyles();

        if (usernameTextField.getText().trim().isEmpty())
        {
            usernameTextField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        if (emailTextField.getText().trim().isEmpty() || !isValidEmail(emailTextField.getText().trim()))
        {
            emailTextField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        if (nameTextField.getText().trim().isEmpty())
        {
            nameTextField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        if (lastnameTextField.getText().trim().isEmpty())
        {
            lastnameTextField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        if (telephoneTextField.getText().trim().isEmpty() || !isValidTelephone(telephoneTextField.getText().trim()))
        {
            telephoneTextField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        if (passwordPasswordField.getText().trim().isEmpty() || !isValidPassword(passwordPasswordField.getText().trim()))
        {
            passwordPasswordField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        String card = cardNumber1TextField.getText() + cardNumber2TextField.getText()
                + cardNumber3TextField.getText() + cardNumber4TextField.getText();

        if (card.isEmpty() || card.length() != 16)
        {
            cardNumber1TextField.setStyle(ERROR_STYLE);
            cardNumber2TextField.setStyle(ERROR_STYLE);
            cardNumber3TextField.setStyle(ERROR_STYLE);
            cardNumber4TextField.setStyle(ERROR_STYLE);
            isValid = false;
        }

        return isValid;
    }

    /**
     * Resets the visual style of all input fields to their normal state. This method removes any error styling applied during validation and restores the default appearance of all form fields.
     */
    private void resetFieldStyles()
    {
        usernameTextField.setStyle(NORMAL_STYLE);
        emailTextField.setStyle(NORMAL_STYLE);
        nameTextField.setStyle(NORMAL_STYLE);
        lastnameTextField.setStyle(NORMAL_STYLE);
        telephoneTextField.setStyle(NORMAL_STYLE);
        passwordPasswordField.setStyle(NORMAL_STYLE);
        cardNumber1TextField.setStyle(NORMAL_STYLE);
        cardNumber2TextField.setStyle(NORMAL_STYLE);
        cardNumber3TextField.setStyle(NORMAL_STYLE);
        cardNumber4TextField.setStyle(NORMAL_STYLE);
    }

    /**
     * Validates an email address format. This method checks if the provided email string matches the standard email format pattern including local part, @ symbol, domain, and top-level domain.
     *
     * @param email the email address string to validate
     * @return true if the email address matches the required format, false otherwise
     */
    private boolean isValidEmail(String email)
    {
        return email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    }

    /**
     * Validates a telephone number format. This method checks if the provided telephone string contains exactly 9 numeric digits.
     *
     * @param telephone the telephone number string to validate
     * @return true if the telephone number matches the required format, false otherwise
     */
    private boolean isValidTelephone(String telephone)
    {
        return telephone.matches("^[0-9]{9}$");
    }

    /**
     * Validates a password according to security requirements. This method checks if the password meets the minimum security standards including length, character diversity, and complexity. The password must contain at least 8 characters, including one uppercase letter, one lowercase letter, and one number.
     *
     * @param password the password string to validate
     * @return true if the password meets all security requirements, false otherwise
     */
    private boolean isValidPassword(String password)
    {
        return password.matches("^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z]).{8,}$");
    }

    /**
     * Configures the telephone text field with input validation and formatting. This method adds a text change listener that restricts input to numeric characters only and enforces a maximum length of 9 digits for telephone numbers.
     */
    private void configureTelephone()
    {
        telephoneTextField.textProperty().addListener((obs, oldValue, newValue) ->
        {
            if (!newValue.matches("\\d*"))
            {
                telephoneTextField.setText(newValue.replaceAll("[^\\d]", ""));
                return;
            }

            if (newValue.length() > 9)
            {
                telephoneTextField.setText(oldValue);
            }
        });
    }

    /**
     * Configures the credit card number text fields with input validation and navigation. This method sets up text change listeners for all four card number segments that enforce numeric-only input, limit each segment to 4 characters, and provide automatic navigation between segments when filled or emptied.
     */
    private void configureCardNumber()
    {
        TextField[] cardFields =
        {
            cardNumber1TextField, cardNumber2TextField, cardNumber3TextField, cardNumber4TextField
        };

        for (int i = 0; i < cardFields.length; i++)
        {
            final TextField currentField = cardFields[i];
            final TextField prevField = (i > 0) ? cardFields[i - 1] : null;
            final TextField nextField = (i < cardFields.length - 1) ? cardFields[i + 1] : null;

            currentField.textProperty().addListener((obs, oldValue, newValue) ->
            {
                // Filter for only numbers
                if (!newValue.matches("\\d*"))
                {
                    currentField.setText(newValue.replaceAll("[^\\d]", ""));
                    return;
                }

                // Filter for no more than 4 characters
                if (newValue.length() > 4)
                {
                    currentField.setText(oldValue);
                    return;
                }

                // Filter for change TextField when there are 4 characters
                if (newValue.length() == 4 && nextField != null)
                {
                    nextField.requestFocus();
                    nextField.positionCaret(nextField.getText().length()); // When change to the next TextField dont select all the content 
                }

                // Filter to change TextField when you deleted all the characters
                if (newValue.isEmpty() && prevField != null)
                {
                    prevField.requestFocus();
                    prevField.positionCaret(prevField.getText().length()); // When change to the previous TextField dont select all the content
                }
            });
        }
    }

    /**
     * Initializes the controller class and sets up input validation handlers. This method is automatically called after the FXML file has been loaded and configures the input validation for telephone and credit card number fields.
     *
     * @param url the location used to resolve relative paths for the root object, or null if the location is not known
     * @param rb the resources used to localize the root object, or null if the root object was not localized
     */
    @Override
    public void initialize(URL url, ResourceBundle rb)
    {
        configureCardNumber();
        configureTelephone();
    }
}
