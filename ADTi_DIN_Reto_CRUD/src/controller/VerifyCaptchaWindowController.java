package controller;

import exception.OurException;
import exception.ShowAlert;
import java.net.URL;
import java.util.Random;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.LoggedProfile;
import model.Profile;

/**
 * Controller class for the CAPTCHA Verification Window interface. This class handles the security verification process using CAPTCHA codes for sensitive operations, particularly user account deletion. It generates random verification codes, validates user input, and executes the deletion operation upon successful verification.
 *
 * The controller implements JavaFX Initializable interface to properly initialize the UI components and manage the CAPTCHA verification workflow.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class VerifyCaptchaWindowController implements Initializable
{

    private Controller controller;
    private Profile profile;
    private int code, userDelete;
    private Runnable onUserDeletedCallback;

    @FXML
    private Pane rightPane;
    @FXML
    private Button confirmBttn;
    @FXML
    private Button cancelBttn;
    @FXML
    private Label username;
    @FXML
    private Label titleLabel;
    @FXML
    private Label codeLabel;
    @FXML
    private Label errorLabel;
    @FXML
    private TextField codeTextField;

    /**
     * Initializes the controller with necessary dependencies and user data. This method sets up the main controller reference, identifies the user to be deleted, displays the current user's username, and generates the initial CAPTCHA code. It functions similarly to a constructor for the controller setup.
     *
     * @param controller the main application controller that manages business logic and data operations
     * @param userDelete the unique identifier of the user to be deleted, or -1 if deleting the currently logged-in user
     */
    public void setController(Controller controller, int userDelete)
    {
        this.controller = controller;
        this.userDelete = userDelete;

        profile = LoggedProfile.getInstance().getProfile();
        username.setText(profile.getUsername());

        generateAndDisplayCode();
    }

    /**
     * Sets the callback function to be executed after successful user deletion. This method allows the parent controller to specify actions that should be performed once the user deletion process is completed successfully, such as navigation, UI updates, or cleanup operations.
     *
     * @param callback the runnable function to execute after user deletion
     */
    public void setOnUserDeletedCallback(Runnable callback)
    {
        onUserDeletedCallback = callback;
    }

    /**
     * Generates a new random CAPTCHA code and updates the display. This method creates a 4-digit random verification code, displays it to the user, clears any previous error messages, and resets the input field to prepare for new user input.
     */
    private void generateAndDisplayCode()
    {
        code = new Random().nextInt(10000);
        codeLabel.setText(String.valueOf(code));
        codeTextField.clear();
    }

    /**
     * Handles the confirmation action when the confirm button is clicked. This method validates the user-input CAPTCHA code against the generated code, executes the user deletion operation upon successful verification, and triggers the callback function if provided. It also handles error cases and displays appropriate feedback to the user.
     *
     * @throws OurException if the user deletion operation fails due to database constraints, data access issues, or system errors
     */
    @FXML
    private void confirmButton()
    {
        String inputCode = codeTextField.getText().trim();

        if (inputCode.isEmpty())
        {
            errorLabel.setText("Please enter the code.");
            return;
        }

        if (!inputCode.equals(String.valueOf(code)))
        {
            errorLabel.setText("Incorrect code. Try again.");
            generateAndDisplayCode();
            return;
        }

        try
        {
            boolean success = controller.deleteUser(userDelete != -1 ? userDelete : profile.getId());

            if (success)
            {
                if (onUserDeletedCallback != null)
                {
                    onUserDeletedCallback.run();
                }

                ShowAlert.showAlert("Success", "User deleted successfully.", Alert.AlertType.INFORMATION);

                ((Stage) confirmBttn.getScene().getWindow()).close();
            }
            else
            {
                ShowAlert.showAlert("Error", "User could not be deleted.", Alert.AlertType.ERROR);
            }
        }
        catch (OurException ex)
        {
            ShowAlert.showAlert("Error", ex.getMessage(), Alert.AlertType.ERROR);
        }
    }

    /**
     * Handles the cancellation action when the cancel button is clicked. This method closes the CAPTCHA verification window without performing the deletion operation, effectively aborting the verification process.
     */
    public void cancelButton()
    {
        Stage stage = (Stage) cancelBttn.getScene().getWindow();
        stage.close();
    }

    /**
     * Initializes the controller class after the FXML file has been loaded. This method is automatically called by JavaFX after the FXML document has been processed and can be used to set up additional UI components or event handlers that are not defined in the FXML file.
     *
     * @param url the location used to resolve relative paths for the root object, or null if the location is not known
     * @param rb the resources used to localize the root object, or null if the root object was not localized
     */
    @Override
    public void initialize(URL url, ResourceBundle rb)
    {
    }
}
