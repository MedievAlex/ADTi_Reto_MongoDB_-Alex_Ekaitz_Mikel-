package unitTests;

import model.Gender;
import model.Profile;
import model.User;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test class for User
 */
public class UserTest
{

    private User userWithId;
    private User userWithoutId;
    private User userWithOtherGender;

    @Before
    public void setUp()
    {
        userWithId = new User(1, "user@test.com", "user123", "password123",
                "John", "Doe", "123456789", Gender.MALE, "1234567890123456");

        userWithoutId = new User("user2@test.com", "user456", "password456",
                "Jane", "Smith", "987654321", Gender.FEMALE, "9876543210987654");

        userWithOtherGender = new User(3, "other@test.com", "otheruser", "pass123",
                "Alex", "Taylor", "555555555", Gender.OTHER, "1111222233334444");
    }

    /**
     * Tests constructor with all parameters including ID
     */
    @Test
    public void testConstructorWithId()
    {
        assertEquals(1, userWithId.getId());
        assertEquals("user@test.com", userWithId.getEmail());
        assertEquals("user123", userWithId.getUsername());
        assertEquals("password123", userWithId.getPassword());
        assertEquals("John", userWithId.getName());
        assertEquals("Doe", userWithId.getLastname());
        assertEquals("123456789", userWithId.getTelephone());
        assertEquals(Gender.MALE, userWithId.getGender());
        assertEquals("1234567890123456", userWithId.getCard());
    }

    /**
     * Tests constructor without ID parameter
     */
    @Test
    public void testConstructorWithoutId()
    {
        assertEquals(-1, userWithoutId.getId());
        assertEquals("user2@test.com", userWithoutId.getEmail());
        assertEquals("user456", userWithoutId.getUsername());
        assertEquals("password456", userWithoutId.getPassword());
        assertEquals("Jane", userWithoutId.getName());
        assertEquals("Smith", userWithoutId.getLastname());
        assertEquals("987654321", userWithoutId.getTelephone());
        assertEquals(Gender.FEMALE, userWithoutId.getGender());
        assertEquals("9876543210987654", userWithoutId.getCard());
    }

    /**
     * Tests getGender method
     */
    @Test
    public void testGetGender()
    {
        Gender result = userWithOtherGender.getGender();
        assertEquals(Gender.OTHER, result);
    }

    /**
     * Tests setGender method
     */
    @Test
    public void testSetGender()
    {
        userWithId.setGender(Gender.FEMALE);
        Gender result = userWithId.getGender();

        assertEquals(Gender.FEMALE, result);
    }

    /**
     * Tests setGender with null value
     */
    @Test
    public void testSetGenderNull()
    {
        userWithId.setGender(null);
        Gender result = userWithId.getGender();

        assertNull(result);
    }

    /**
     * Tests getCard method
     */
    @Test
    public void testGetCard()
    {
        String result = userWithId.getCard();
        assertEquals("1234567890123456", result);
    }

    /**
     * Tests setCard method
     */
    @Test
    public void testSetCard()
    {
        userWithId.setCard("new_card_123456");
        String result = userWithId.getCard();

        assertEquals("new_card_123456", result);
    }

    /**
     * Tests setCard with null value
     */
    @Test
    public void testSetCardNull()
    {
        userWithId.setCard(null);
        String result = userWithId.getCard();

        assertNull(result);
    }

    /**
     * Tests toString method
     */
    @Test
    public void testToString()
    {
        String result = userWithId.toString();
        assertEquals("user123", result);

        String result2 = userWithoutId.toString();
        assertEquals("user456", result2);
    }

    /**
     * Tests show method with valid card
     */
    @Test
    public void testShowWithCard()
    {
        String result = userWithId.show();
        assertTrue(result.contains("User {"));
        assertTrue(result.contains("user123"));
        assertTrue(result.contains("Gender: MALE"));
        assertTrue(result.contains("**** **** **** 3456"));
    }

    /**
     * Tests show method with null card
     */
    @Test
    public void testShowWithNullCard()
    {
        User userWithNullCard = new User(4, "nullcard@test.com", "nulluser", "pass",
                "Null", "Card", "444444444", Gender.FEMALE, null);

        String result = userWithNullCard.show();
        assertTrue(result.contains("User {"));
        assertTrue(result.contains("nulluser"));
        assertTrue(result.contains("Gender: FEMALE"));
        assertFalse(result.contains("Card:"));
    }

    /**
     * Tests inheritance from Profile class
     */
    @Test
    public void testUserIsInstanceOfProfile()
    {
        assertTrue(userWithId instanceof Profile);
        assertTrue(userWithoutId instanceof Profile);
    }

    /**
     * Tests all gender enum values
     */
    @Test
    public void testAllGenderValues()
    {
        assertEquals(Gender.MALE, userWithId.getGender());
        assertEquals(Gender.FEMALE, userWithoutId.getGender());
        assertEquals(Gender.OTHER, userWithOtherGender.getGender());
    }
}
