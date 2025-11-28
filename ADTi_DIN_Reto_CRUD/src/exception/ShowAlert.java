package exception;

import javafx.scene.control.Alert;

/**
 * Utility class for displaying alert dialogs in the JavaFX application. This class provides a centralized and simplified way to show various types of alert messages to users, including information, warning, error, and confirmation dialogs.
 *
 * The class contains static methods that can be called from anywhere in the application without requiring instantiation, making it convenient for consistent user feedback.
 */
public class ShowAlert
{

    /**
     * Displays a modal alert dialog with the specified parameters. This method creates and shows an alert dialog with the given title, message content, and alert type. The dialog is modal and blocks user interaction with other application windows until dismissed.
     *
     * @param title the title text to display in the alert dialog header
     * @param message the main content text to display in the alert dialog body
     * @param type the type of alert which determines the default icon and behavior; common types include INFORMATION, WARNING, ERROR, and CONFIRMATION
     */
    public static void showAlert(String title, String message, Alert.AlertType type)
    {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
}
