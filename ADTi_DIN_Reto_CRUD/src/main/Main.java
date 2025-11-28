package main;

import controller.Controller;
import javafx.application.Application;
import javafx.stage.Stage;

/**
 * Main application class that serves as the entry point for the JavaFX application. This class extends JavaFX Application and is responsible for initializing the primary stage, creating the main controller, and launching the user interface.
 *
 * The application follows the Model-View-Controller (MVC) pattern and initializes the login window as the starting point of the user interface.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public class Main extends Application
{

    /**
     * The main entry point for all JavaFX applications. This method is called after the init method has returned, and after the system is ready for the application to begin running. It creates the main controller instance and displays the login window.
     *
     * @param stage the primary stage for this application, onto which the application scene can be set
     * @throws Exception if the application initialization fails, including controller creation errors or window display issues
     */
    @Override
    public void start(Stage stage) throws Exception
    {
        Controller controller = new Controller();
        controller.showWindow(stage);
    }

    /**
     * The main method that launches the JavaFX application. This method is ignored in correctly deployed JavaFX application. Instead, the main class should be specified in the JAR file manifest or through other deployment mechanisms.
     *
     * @param args the command line arguments passed to the application. These arguments can be used to configure application behavior
     */
    public static void main(String[] args)
    {
        launch(args);
    }
}
