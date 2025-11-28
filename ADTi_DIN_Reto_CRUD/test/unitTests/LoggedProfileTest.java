package unitTests;

import model.Admin;
import model.LoggedProfile;
import model.Profile;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

/**
 * Test class for LoggedProfile
 */
public class LoggedProfileTest
{

    private Profile testProfile;

    /**
     * Sets up test data before each test
     */
    @Before
    public void setUp()
    {
        testProfile = new Admin(1, "test@email.com", "testuser", "password",
                "Test", "User", "123456789", "1234567890123456");
        LoggedProfile.getInstance().clear();
    }

    /**
     * Tests getInstance method returns same instance
     */
    @Test
    public void testGetInstanceReturnsSameInstance()
    {
        LoggedProfile instance1 = LoggedProfile.getInstance();
        LoggedProfile instance2 = LoggedProfile.getInstance();

        assertSame(instance1, instance2);
    }

    /**
     * Tests setProfile and getProfile methods
     */
    @Test
    public void testSetAndGetProfile()
    {
        LoggedProfile loggedProfile = LoggedProfile.getInstance();

        loggedProfile.setProfile(testProfile);
        Profile result = loggedProfile.getProfile();

        assertEquals(testProfile, result);
    }

    /**
     * Tests getProfile when no profile is set
     */
    @Test
    public void testGetProfileWhenNull()
    {
        LoggedProfile loggedProfile = LoggedProfile.getInstance();
        loggedProfile.clear();

        Profile result = loggedProfile.getProfile();

        assertNull(result);
    }

    /**
     * Tests clear method
     */
    @Test
    public void testClear()
    {
        LoggedProfile loggedProfile = LoggedProfile.getInstance();
        loggedProfile.setProfile(testProfile);

        assertNotNull(loggedProfile.getProfile());

        loggedProfile.clear();
        assertNull(loggedProfile.getProfile());
    }

    /**
     * Tests multiple setProfile calls
     */
    @Test
    public void testMultipleSetProfile()
    {
        LoggedProfile loggedProfile = LoggedProfile.getInstance();

        Profile profile1 = new Admin(1, "test1@email.com", "user1", "pass1",
                "Name1", "Last1", "111111111", "1111111111111111");
        Profile profile2 = new Admin(2, "test2@email.com", "user2", "pass2",
                "Name2", "Last2", "222222222", "2222222222222222");

        loggedProfile.setProfile(profile1);
        assertEquals(profile1, loggedProfile.getProfile());

        loggedProfile.setProfile(profile2);
        assertEquals(profile2, loggedProfile.getProfile());
    }

    /**
     * Tests singleton persistence across multiple calls
     */
    @Test
    public void testSingletonPersistence()
    {
        LoggedProfile instance1 = LoggedProfile.getInstance();
        instance1.setProfile(testProfile);

        LoggedProfile instance2 = LoggedProfile.getInstance();
        Profile result = instance2.getProfile();

        assertEquals(testProfile, result);
    }
}
