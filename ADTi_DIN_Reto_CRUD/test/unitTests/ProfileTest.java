package unitTests;

import model.Profile;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test class for Profile
 */
public class ProfileTest
{

    private static class TestProfile extends Profile
    {

        public TestProfile()
        {
            super();
        }

        public TestProfile(int id, String email, String username, String password,
                String name, String lastname, String telephone)
        {
            super(id, email, username, password, name, lastname, telephone);
        }

        public TestProfile(String email, String username, String password,
                String name, String lastname, String telephone)
        {
            super(email, username, password, name, lastname, telephone);
        }

        @Override
        public String show()
        {
            return "TestProfile: " + getUsername();
        }
    }

    private TestProfile defaultProfile;
    private TestProfile profileWithId;
    private TestProfile profileWithoutId;

    @Before
    public void setUp()
    {
        defaultProfile = new TestProfile();
        profileWithId = new TestProfile(1, "test@email.com", "testuser",
                "password123", "John", "Doe", "123456789");
        profileWithoutId = new TestProfile("test@email.com", "testuser",
                "password123", "John", "Doe", "123456789");
    }

    /**
     * Tests default constructor
     */
    @Test
    public void testDefaultConstructor()
    {
        assertEquals(-1, defaultProfile.getId());
        assertEquals("", defaultProfile.getEmail());
        assertEquals("", defaultProfile.getUsername());
        assertEquals("", defaultProfile.getPassword());
        assertEquals("", defaultProfile.getName());
        assertEquals("", defaultProfile.getLastname());
        assertEquals("", defaultProfile.getTelephone());
    }

    /**
     * Tests constructor with all parameters including ID
     */
    @Test
    public void testConstructorWithId()
    {
        assertEquals(1, profileWithId.getId());
        assertEquals("test@email.com", profileWithId.getEmail());
        assertEquals("testuser", profileWithId.getUsername());
        assertEquals("password123", profileWithId.getPassword());
        assertEquals("John", profileWithId.getName());
        assertEquals("Doe", profileWithId.getLastname());
        assertEquals("123456789", profileWithId.getTelephone());
    }

    /**
     * Tests constructor without ID parameter
     */
    @Test
    public void testConstructorWithoutId()
    {
        assertEquals(-1, profileWithoutId.getId());
        assertEquals("test@email.com", profileWithoutId.getEmail());
        assertEquals("testuser", profileWithoutId.getUsername());
        assertEquals("password123", profileWithoutId.getPassword());
        assertEquals("John", profileWithoutId.getName());
        assertEquals("Doe", profileWithoutId.getLastname());
        assertEquals("123456789", profileWithoutId.getTelephone());
    }

    /**
     * Tests setId and getId methods
     */
    @Test
    public void testSetAndGetId()
    {
        defaultProfile.setId(5);
        assertEquals(5, defaultProfile.getId());
    }

    /**
     * Tests setEmail and getEmail methods
     */
    @Test
    public void testSetAndGetEmail()
    {
        defaultProfile.setEmail("new@email.com");
        assertEquals("new@email.com", defaultProfile.getEmail());
    }

    /**
     * Tests setUsername and getUsername methods
     */
    @Test
    public void testSetAndGetUsername()
    {
        defaultProfile.setUsername("newusername");
        assertEquals("newusername", defaultProfile.getUsername());
    }

    /**
     * Tests setPassword and getPassword methods
     */
    @Test
    public void testSetAndGetPassword()
    {
        defaultProfile.setPassword("newpassword");
        assertEquals("newpassword", defaultProfile.getPassword());
    }

    /**
     * Tests setName and getName methods
     */
    @Test
    public void testSetAndGetName()
    {
        defaultProfile.setName("New Name");
        assertEquals("New Name", defaultProfile.getName());
    }

    /**
     * Tests setLastname and getLastname methods
     */
    @Test
    public void testSetAndGetLastname()
    {
        defaultProfile.setLastname("New Lastname");
        assertEquals("New Lastname", defaultProfile.getLastname());
    }

    /**
     * Tests setTelephone and getTelephone methods
     */
    @Test
    public void testSetAndGetTelephone()
    {
        defaultProfile.setTelephone("987654321");
        assertEquals("987654321", defaultProfile.getTelephone());
    }

    /**
     * Tests toString method
     */
    @Test
    public void testToString()
    {
        String result = profileWithId.toString();
        assertTrue(result.contains("ID: 1"));
        assertTrue(result.contains("Email: test@email.com"));
        assertTrue(result.contains("Username: testuser"));
        assertTrue(result.contains("Password: password123"));
        assertTrue(result.contains("Name: John"));
        assertTrue(result.contains("Last name: Doe"));
        assertTrue(result.contains("Telephone: 123456789"));
    }

    /**
     * Tests abstract method implementation
     */
    @Test
    public void testAbstractMethod()
    {
        String result = defaultProfile.show();
        assertEquals("TestProfile: ", result);
    }

    /**
     * Tests multiple setters combination
     */
    @Test
    public void testMultipleSetters()
    {
        TestProfile profile = new TestProfile();
        profile.setId(10);
        profile.setEmail("multi@test.com");
        profile.setUsername("multiuser");
        profile.setName("Multi");
        profile.setLastname("Test");

        assertEquals(10, profile.getId());
        assertEquals("multi@test.com", profile.getEmail());
        assertEquals("multiuser", profile.getUsername());
        assertEquals("Multi", profile.getName());
        assertEquals("Test", profile.getLastname());
    }
}
