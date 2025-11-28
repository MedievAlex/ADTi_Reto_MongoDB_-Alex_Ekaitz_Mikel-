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
import javafx.scene.control.Button;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import javafx.scene.control.Alert;
import model.Profile;
import model.User;

/**
 * Controller class for the Login Window interface. This class handles user authentication and navigation between the login screen and other application windows. It manages the login process, validates user credentials, and redirects users to appropriate interfaces based on their profile type (User or Admin).
 *
 * The controller implements JavaFX Initializable interface to properly initialize the UI components and set up event handlers for user interactions.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class LoginWindowController implements Initializable
{

    private Controller controller;

    @FXML
    private Pane leftPane;
    @FXML
    private Pane rightPane;
    @FXML
    private TextField credentialTextField;
    @FXML
    private PasswordField passwordPasswordField;
    @FXML
    private Button logInBttn;
    @FXML
    private Button signUpBttn;

    /**
     * Sets the main controller for this login window controller. This method establishes the connection to the main application controller that handles business logic and data operations.
     *
     * @param controller the main application controller that manages business logic and data operations
     */
    public void setController(Controller controller)
    {
        this.controller = controller;
    }

    /**
     * Handles the user login process when the login button is clicked. This method validates input fields, authenticates user credentials through the main controller, and navigates to the appropriate user interface (User Window or Admin Window) based on the authenticated profile type.
     *
     * <p>
     * The method performs the following steps:
     * <ol>
     * <li>Validates that both credential and password fields are not empty</li>
     * <li>Attempts authentication through the main controller</li>
     * <li>Redirects to User Window for regular users or Admin Window for administrators</li>
     * <li>Displays appropriate error messages for authentication failures or exceptions</li>
     * </ol>
     * </p>
     *
     * @throws OurException if authentication fails due to invalid credentials or system errors
     * @throws IOException if there is an error loading the destination window FXML file
     */
    @FXML
    private void handleLogin()
    {
        String credential = credentialTextField.getText();
        String password = passwordPasswordField.getText();

        if (credential.isEmpty() || password.isEmpty())
        {
            ShowAlert.showAlert("Error", "Please fill all the fields.", Alert.AlertType.ERROR);
            return;
        }

        try
        {
            Profile loggedIn = controller.login(credential, password);

            if (loggedIn != null)
            {
                boolean isUser = loggedIn instanceof User;
                String fxmlPath = isUser ? "/view/UserWindow.fxml" : "/view/AdminWindow.fxml";
                FXMLLoader loader = new FXMLLoader(getClass().getResource(fxmlPath));
                Parent window = loader.load();

                if (isUser)
                {
                    ((UserWindowController) loader.getController()).setController(this.controller);
                }
                else
                {
                    ((AdminWindowController) loader.getController()).setController(this.controller);
                }

                Stage currentwindow = (Stage) logInBttn.getScene().getWindow();
                currentwindow.setTitle(isUser ? "User" : "Admin");
                currentwindow.setScene(new Scene(window));
            }
            else
            {
                ShowAlert.showAlert("Error", "Incorrect credentials.", Alert.AlertType.ERROR);
            }

        }
        catch (OurException ex)
        {
            ShowAlert.showAlert("Error", ex.getMessage(), Alert.AlertType.ERROR);
        }
        catch (IOException ex)
        {
            ShowAlert.showAlert("Error", "Error opening window.", Alert.AlertType.ERROR);
        }
    }

    /**
     * Opens the Sign Up window for new user registration. This method navigates from the login screen to the user registration interface, allowing new users to create accounts in the system.
     *
     * <p>
     * The method loads the Sign Up window FXML file, sets up the corresponding controller, and transitions the current window to display the registration form.</p>
     *
     */
    @FXML
    public void openSignUp()
    {
        try
        {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/SignUpWindow.fxml"));
            Parent parentWindow = loader.load();

            SignUpWindowController nextController = loader.getController();
            nextController.setController(controller);

            Stage actualWindow = (Stage) signUpBttn.getScene().getWindow();
            actualWindow.setTitle("Sign Up");
            actualWindow.setResizable(false);
            actualWindow.setScene(new Scene(parentWindow));
        }
        catch (IOException ex)
        {
            ShowAlert.showAlert("Error", "Error opening Sign Up window.", Alert.AlertType.ERROR);
        }
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
        // TODO
    }
}
