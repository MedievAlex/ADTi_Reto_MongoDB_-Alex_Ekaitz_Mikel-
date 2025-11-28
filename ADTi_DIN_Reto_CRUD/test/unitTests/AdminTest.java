package unitTests;

import model.Admin;
import model.Profile;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test class for Admin
 */
public class AdminTest
{

    private Admin adminWithId;
    private Admin adminWithoutId;

    @Before
    public void setUp()
    {
        adminWithId = new Admin(1, "admin@test.com", "adminUser", "password123",
                "John", "Doe", "123456789", "1234567890123456");

        adminWithoutId = new Admin("admin2@test.com", "adminUser2", "password456",
                "Jane", "Smith", "987654321", "9876543210987654");
    }

    /**
     * Tests constructor with all parameters including ID
     */
    @Test
    public void testConstructorWithId()
    {
        assertEquals(1, adminWithId.getId());
        assertEquals("admin@test.com", adminWithId.getEmail());
        assertEquals("adminUser", adminWithId.getUsername());
        assertEquals("password123", adminWithId.getPassword());
        assertEquals("John", adminWithId.getName());
        assertEquals("Doe", adminWithId.getLastname());
        assertEquals("123456789", adminWithId.getTelephone());
        assertEquals("1234567890123456", adminWithId.getCurrent_account());
    }

    /**
     * Tests constructor without ID parameter
     */
    @Test
    public void testConstructorWithoutId()
    {
        assertEquals(-1, adminWithoutId.getId());
        assertEquals("admin2@test.com", adminWithoutId.getEmail());
        assertEquals("adminUser2", adminWithoutId.getUsername());
        assertEquals("password456", adminWithoutId.getPassword());
        assertEquals("Jane", adminWithoutId.getName());
        assertEquals("Smith", adminWithoutId.getLastname());
        assertEquals("987654321", adminWithoutId.getTelephone());
        assertEquals("9876543210987654", adminWithoutId.getCurrent_account());
    }

    /**
     * Tests getCurrent_account method
     */
    @Test
    public void testGetCurrentAccount()
    {
        String result = adminWithId.getCurrent_account();
        assertEquals("1234567890123456", result);
    }

    /**
     * Tests setCurrent_account method
     */
    @Test
    public void testSetCurrentAccount()
    {
        adminWithId.setCurrent_account("new_account_123456");
        String result = adminWithId.getCurrent_account();
        assertEquals("new_account_123456", result);
    }

    /**
     * Tests setCurrent_account with null value
     */
    @Test
    public void testSetCurrentAccountNull()
    {
        adminWithId.setCurrent_account(null);
        String result = adminWithId.getCurrent_account();
        assertNull(result);
    }

    /**
     * Tests show method with valid current account
     */
    @Test
    public void testShowWithCurrentAccount()
    {
        String result = adminWithId.show();
        assertTrue(result.contains("Admin {"));
        assertTrue(result.contains("adminUser"));
        assertTrue(result.contains("**** **** **** 3456"));
    }

    /**
     * Tests show method with null current account
     */
    @Test
    public void testShowWithNullCurrentAccount()
    {
        Admin adminWithNullAccount = new Admin(2, "nulladmin@test.com", "nulladmin", "pass",
                "Null", "Admin", "444444444", null);

        String result = adminWithNullAccount.show();
        assertTrue(result.contains("Admin {"));
        assertTrue(result.contains("nulladmin"));
        assertNotNull(result);
    }

    /**
     * Tests inheritance from Profile class
     */
    @Test
    public void testAdminIsInstanceOfProfile()
    {
        assertTrue(adminWithId instanceof Profile);
        assertTrue(adminWithoutId instanceof Profile);
    }

    /**
     * Tests current account number masking with different valid accounts
     */
    @Test
    public void testCurrentAccountMaskingWithDifferentAccounts()
    {
        Admin admin1 = new Admin(3, "test1@test.com", "admin1", "pass",
                "Test1", "Admin1", "111111111", "1111222233334444");
        Admin admin2 = new Admin(4, "test2@test.com", "admin2", "pass",
                "Test2", "Admin2", "222222222", "9999888877776666");

        String result1 = admin1.show();
        String result2 = admin2.show();

        assertTrue(result1.contains("**** **** **** 4444"));
        assertTrue(result2.contains("**** **** **** 6666"));
    }
}
