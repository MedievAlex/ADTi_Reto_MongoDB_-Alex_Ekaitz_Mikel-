package model;

/**
 * Abstract base class representing a user profile in the system. This class defines the common attributes and behavior shared by all types of user profiles, including both regular users and administrators.
 *
 * Profile serves as the foundation for user identity management and provides the core personal information storage and retrieval functionality for the application.
 *
 * @author Kevin, Alex, Victor, Ekaitz
 */
public abstract class Profile
{

    protected int p_id;
    protected String p_email;
    protected String p_username;
    protected String p_password;
    protected String p_name;
    protected String p_lastname;
    protected String p_telephone;

    /**
     * Default constructor that initializes all profile attributes to empty values. The ID is set to -1 to indicate an unpersisted profile that hasn't been assigned a database identifier yet.
     */
    public Profile()
    {
        this.p_id = -1;
        this.p_email = "";
        this.p_username = "";
        this.p_password = "";
        this.p_name = "";
        this.p_lastname = "";
        this.p_telephone = "";
    }

    /**
     * Constructs a Profile with all attributes including the database ID. This constructor is typically used when loading existing profile data from persistent storage where the ID has already been assigned.
     *
     * @param p_id the unique identifier for the profile
     * @param p_email the email address associated with the profile
     * @param p_username the username used for authentication
     * @param p_password the password used for authentication
     * @param p_name the first name of the profile owner
     * @param p_lastname the last name of the profile owner
     * @param p_telephone the telephone number of the profile owner
     */
    public Profile(int p_id, String p_email, String p_username, String p_password, String p_name, String p_lastname, String p_telephone)
    {
        this.p_id = p_id;
        this.p_email = p_email;
        this.p_username = p_username;
        this.p_password = p_password;
        this.p_name = p_name;
        this.p_lastname = p_lastname;
        this.p_telephone = p_telephone;
    }

    /**
     * Constructs a Profile without the database ID. This constructor is typically used when creating new profiles that haven't been persisted to the database yet and will receive an ID upon saving.
     *
     * @param p_email the email address associated with the profile
     * @param p_username the username used for authentication
     * @param p_password the password used for authentication
     * @param p_name the first name of the profile owner
     * @param p_lastname the last name of the profile owner
     * @param p_telephone the telephone number of the profile owner
     */
    public Profile(String p_email, String p_username, String p_password, String p_name, String p_lastname, String p_telephone)
    {
        this.p_id = -1;
        this.p_email = p_email;
        this.p_username = p_username;
        this.p_password = p_password;
        this.p_name = p_name;
        this.p_lastname = p_lastname;
        this.p_telephone = p_telephone;
    }

    /**
     * Returns the unique identifier of the profile.
     *
     * @return the profile ID, or -1 if the profile hasn't been persisted
     */
    public int getId()
    {
        return p_id;
    }

    /**
     * Sets the unique identifier for the profile.
     *
     * @param p_id the new ID to assign to the profile
     */
    public void setId(int p_id)
    {
        this.p_id = p_id;
    }

    /**
     * Returns the email address associated with the profile.
     *
     * @return the profile email address
     */
    public String getEmail()
    {
        return p_email;
    }

    /**
     * Sets the email address for the profile.
     *
     * @param p_email the new email address to assign to the profile
     */
    public void setEmail(String p_email)
    {
        this.p_email = p_email;
    }

    /**
     * Returns the username used for authentication.
     *
     * @return the profile username
     */
    public String getUsername()
    {
        return p_username;
    }

    /**
     * Sets the username for the profile.
     *
     * @param p_username the new username to assign to the profile
     */
    public void setUsername(String p_username)
    {
        this.p_username = p_username;
    }

    /**
     * Returns the password used for authentication.
     *
     * @return the profile password
     */
    public String getPassword()
    {
        return p_password;
    }

    /**
     * Sets the password for the profile.
     *
     * @param p_password the new password to assign to the profile
     */
    public void setPassword(String p_password)
    {
        this.p_password = p_password;
    }

    /**
     * Returns the first name of the profile owner.
     *
     * @return the profile owner's first name
     */
    public String getName()
    {
        return p_name;
    }

    /**
     * Sets the first name for the profile owner.
     *
     * @param p_name the new first name to assign to the profile
     */
    public void setName(String p_name)
    {
        this.p_name = p_name;
    }

    /**
     * Returns the last name of the profile owner.
     *
     * @return the profile owner's last name
     */
    public String getLastname()
    {
        return p_lastname;
    }

    /**
     * Sets the last name for the profile owner.
     *
     * @param p_lastname the new last name to assign to the profile
     */
    public void setLastname(String p_lastname)
    {
        this.p_lastname = p_lastname;
    }

    /**
     * Returns the telephone number of the profile owner.
     *
     * @return the profile owner's telephone number
     */
    public String getTelephone()
    {
        return p_telephone;
    }

    /**
     * Sets the telephone number for the profile owner.
     *
     * @param p_telephone the new telephone number to assign to the profile
     */
    public void setTelephone(String p_telephone)
    {
        this.p_telephone = p_telephone;
    }

    /**
     * Returns a string representation of the profile containing all attributes. This method provides a comprehensive textual representation of the profile including all personal information and credentials.
     *
     * @return a string containing all profile attributes
     */
    @Override
    public String toString()
    {
        return "ID: " + p_id + ", Email: " + p_email + ", Username: " + p_username + ", Password: " + p_password + ", Name: " + p_name + ", Last name: " + p_lastname
                + ", Telephone: " + p_telephone;
    }

    /**
     * Abstract method for displaying profile information in a customized format. Concrete subclasses must implement this method to provide their own specific representation of profile data.
     *
     * @return a formatted string representation of the profile
     */
    public abstract String show();
}
