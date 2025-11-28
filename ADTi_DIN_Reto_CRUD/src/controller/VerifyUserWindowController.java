package controller;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import model.LoggedProfile;
import model.Profile;

/**
 * Controller class for the User Verification Window interface. This class handles the initial password verification step for sensitive operations, particularly user account deletion. It serves as the first security layer by requiring the user to confirm their identity with their password before proceeding to additional verification steps.
 *
 * The controller implements JavaFX Initializable interface to properly initialize the UI components and manage the user verification workflow.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class VerifyUserWindowController implements Initializable
{

    private Controller controller;
    private Profile profile;
    private int userDelete;
    private Runnable onUserDeletedCallback;

    @FXML
    private Pane rightPane;
    @FXML
    private PasswordField passwordPasswordField;
    @FXML
    private Label username;
    @FXML
    private Button confirmBttn;
    @FXML
    private Button cancelBttn;
    @FXML
    private Label titleLabel;
    @FXML
    private Label errorLabel;

    /**
     * Initializes the controller with necessary dependencies and user data. This method sets up the main controller reference, identifies the user to be deleted, and displays the current user's username in the interface. It functions similarly to a constructor for the controller setup.
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
     * Handles the confirmation action when the confirm button is clicked. This method validates the user's password against the stored profile password, and if successful, navigates to the next verification step (action confirmation window). It transfers the callback function to the next verification step and displays appropriate error messages for invalid or missing passwords.
     *
     */
    @FXML
    public void confirmButton()
    {
        String password = passwordPasswordField.getText().trim();

        if (password.isEmpty())
        {
            errorLabel.setText("Enter your password.");
            return;
        }

        if (profile.getPassword().equals(password))
        {
            try
            {
                FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/VerifyActionWindow.fxml"));
                Parent parentWindow = loader.load();

                VerifyActionWindowController nextController = loader.getController();
                nextController.setController(controller, userDelete);

                if (onUserDeletedCallback != null)
                {
                    nextController.setOnUserDeletedCallback(onUserDeletedCallback);
                }

                Stage actualWindow = (Stage) confirmBttn.getScene().getWindow();
                actualWindow.setTitle("Verify your Action");
                actualWindow.setResizable(false);
                actualWindow.setScene(new Scene(parentWindow));
            }
            catch (IOException ex)
            {
                errorLabel.setText("Error loading window.");
            }
        }
        else
        {
            errorLabel.setText("Incorrect password.");
        }
    }

    /**
     * Handles the cancellation action when the cancel button is clicked. This method closes the user verification window without performing any further actions, effectively aborting the verification process.
     */
    @FXML
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
