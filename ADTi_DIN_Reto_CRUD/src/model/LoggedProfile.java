package model;

/**
 * Singleton class that manages the currently logged-in user profile throughout the application. This class implements the Singleton pattern to ensure only one instance exists and provides global access to the current user's profile information.
 *
 * The class maintains the authenticated user's profile data during the application session and allows different parts of the application to access user information without passing references throughout the call stack.
 */
public class LoggedProfile
{

    private static LoggedProfile instance;
    private Profile currentProfile;

    private LoggedProfile()
    {
    }

    /**
     * Returns the singleton instance of the LoggedProfile class. This method implements lazy initialization, creating the instance only when it is first requested.
     *
     * @return the single instance of LoggedProfile
     */
    public static LoggedProfile getInstance()
    {
        if (instance == null)
        {
            instance = new LoggedProfile();
        }
        return instance;
    }

    /**
     * Sets the currently logged-in user profile. This method stores the authenticated user's profile information which can then be accessed throughout the application.
     *
     * @param profile the Profile object representing the logged-in user
     */
    public void setProfile(Profile profile)
    {
        this.currentProfile = profile;
    }

    /**
     * Retrieves the currently logged-in user profile. This method returns the Profile object that was previously set during the authentication process.
     *
     * @return the current Profile object, or null if no user is logged in
     */
    public Profile getProfile()
    {
        return currentProfile;
    }

    /**
     * Clears the currently logged-in user profile. This method is typically called during logout to remove the user's profile information from the session and reset the authentication state.
     */
    public void clear()
    {
        this.currentProfile = null;
    }
}
