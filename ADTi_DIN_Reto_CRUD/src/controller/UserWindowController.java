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
import javafx.scene.image.Image;
import javafx.scene.layout.Pane;
import javafx.stage.Modality;
import javafx.stage.Stage;
import model.Gender;
import model.LoggedProfile;
import model.User;

/**
 * Controller class for the User Window interface. This class handles the user profile management functionality, allowing users to view and update their personal information, change passwords, and manage account settings. It provides a comprehensive interface for users to maintain their profile data with proper validation and security measures.
 *
 * The controller implements JavaFX Initializable interface to properly initialize the UI components and set up input validation handlers.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class UserWindowController implements Initializable
{

    private Controller controller;
    private User user;

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
    private Button deleteUserBttn;
    @FXML
    private Pane rightPane;
    @FXML
    private TextField usernameTextField;
    @FXML
    private TextField emailTextField;
    @FXML
    private TextField nameTextField;
    @FXML
    private TextField lastnameTextField;
    @FXML
    private TextField telephoneTextField;
    @FXML
    private PasswordField passwordPasswordField;
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
    private Button saveChangesBttn;
    @FXML
    private Label username;
    @FXML
    private Button logOutBttn;

    private final String ERROR_STYLE = "-fx-border-color: red; -fx-border-width: 2px;";
    private final String NORMAL_STYLE = "-fx-border-color: null;";

    /**
     * Sets the main controller and initializes the user interface with current user data. This method configures the controller reference, retrieves the currently logged-in user profile from the LoggedProfile singleton, displays the username, and populates all form fields with the user's current information.
     *
     * @param controller the main application controller that manages business logic and data operations
     */
    public void setController(Controller controller)
    {
        this.controller = controller;
        user = (User) LoggedProfile.getInstance().getProfile();
        username.setText(user.getUsername());
        setData();
    }

    /**
     * Populates the form fields with the current user's data. This method loads all user information from the User object into the corresponding form fields, including personal details, contact information, and payment card data. It also sets the appropriate gender radio button based on the user's stored gender preference.
     */
    public void setData()
    {
        username.setText(user.getUsername());
        usernameTextField.setText(user.getUsername());
        emailTextField.setText(user.getEmail());
        passwordPasswordField.setText(user.getPassword());
        nameTextField.setText(user.getName());
        lastnameTextField.setText(user.getLastname());
        telephoneTextField.setText(String.valueOf(user.getTelephone()));

        switch (user.getGender())
        {
            case MALE:
                maleRadioButton.setSelected(true);
                break;
            case FEMALE:
                femaleRadioButton.setSelected(true);
                break;
            case OTHER:
                otherRadioButton.setSelected(true);
                break;
        }

        if (user.getCard() != null && user.getCard().length() == 16)
        {
            cardNumber1TextField.setText(user.getCard().substring(0, 4));
            cardNumber2TextField.setText(user.getCard().substring(4, 8));
            cardNumber3TextField.setText(user.getCard().substring(8, 12));
            cardNumber4TextField.setText(user.getCard().substring(12, 16));
        }
    }

    /**
     * Saves the changes made to the user's profile information. This method validates all input fields, updates the user object with the modified data, and persists the changes to the system. Upon successful update, the logged-in profile is refreshed and a success message is displayed.
     *
     */
    @FXML
    public void saveChanges()
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
        String name = nameTextField.getText().trim();
        String lastname = lastnameTextField.getText().trim();
        String telephone = telephoneTextField.getText().trim();
        String password = passwordPasswordField.getText().trim();

        Gender gender = Gender.OTHER;
        if (maleRadioButton.isSelected())
        {
            gender = Gender.MALE;
        }
        else if (femaleRadioButton.isSelected())
        {
            gender = Gender.FEMALE;
        }

        String card = cardNumber1TextField.getText()
                + cardNumber2TextField.getText()
                + cardNumber3TextField.getText()
                + cardNumber4TextField.getText();

        user.setUsername(username);
        user.setEmail(email);
        user.setName(name);
        user.setLastname(lastname);
        user.setTelephone(telephone);
        user.setPassword(password);
        user.setGender(gender);
        user.setCard(card);

        try
        {
            boolean success = controller.updateUser(user);

            if (success)
            {
                LoggedProfile.getInstance().setProfile(user);

                ShowAlert.showAlert("Success", "User updated successfully.", Alert.AlertType.INFORMATION);

                resetFieldStyles();
            }
            else
            {
                ShowAlert.showAlert("Error", "Could not update user.", Alert.AlertType.ERROR);
            }
        }
        catch (OurException ex)
        {
            ShowAlert.showAlert("Error", ex.getMessage(), Alert.AlertType.ERROR);
        }
    }

    /**
     * Initiates the user account deletion process by opening a verification window. This method opens a confirmation dialog that requires additional verification before permanently deleting the user's account. Upon successful deletion, the user is automatically logged out through a callback mechanism.
     */
    @FXML
    public void deleteUser()
    {
        try
        {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/VerifyUserWindow.fxml"));
            Parent root = loader.load();

            VerifyUserWindowController verifyController = loader.getController();
            verifyController.setController(this.controller, -1);

            verifyController.setOnUserDeletedCallback(() ->
            {
                logOut();
            });

            Stage stage = new Stage();
            stage.setScene(new Scene(root));
            stage.setTitle("Verify User");
            stage.setResizable(false);
            stage.getIcons().add(new Image(getClass().getResourceAsStream("/images/logo.png")));
            stage.initModality(Modality.WINDOW_MODAL);
            stage.initOwner(deleteUserBttn.getScene().getWindow());
            stage.show();
        }
        catch (IOException ex)
        {
            ShowAlert.showAlert("Error", "Could not delete user.", Alert.AlertType.ERROR);
        }
    }

    /**
     * Logs out the current user and returns to the login screen. This method clears the logged-in profile, resets user references, and navigates back to the login window. If an error occurs during the logout process, an error alert is displayed to the user.
     */
    @FXML
    public void logOut()
    {
        LoggedProfile.getInstance().clear();
        user = null;

        try
        {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/LoginWindow.fxml"));
            Parent window = loader.load();
            LoginWindowController loginController = loader.getController();
            loginController.setController(this.controller);
            Stage currentwindow = (Stage) logOutBttn.getScene().getWindow();
            currentwindow.setScene(new Scene(window));
        }
        catch (IOException ex)
        {
            ShowAlert.showAlert("Error", "Could not logout.", Alert.AlertType.ERROR);
        }
    }

    /**
     * Validates all input fields in the user profile form. This method checks each required field for proper formatting, completeness, and content according to business rules, applying visual error styling to invalid fields.
     *
     * @return true if all fields pass validation, false if any validation rule is violated
     */
    private boolean validateFields()
    {
        boolean isValid = true;

        resetFieldStyles();

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
