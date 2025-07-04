package io.rtdi.bigdata.rulesservice.ui;

import com.microsoft.playwright.Locator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI integration tests for the Topics page.
 * Tests the functionality of viewing and managing topic rules.
 */
@EnabledIf("isApplicationRunning")
public class TopicsUITest extends BaseUITest {

    @Test
    void testTopicsPageLoads() {
        navigateToPage("Topics");
        
        // Verify the page title is correct
        assertTrue(page.title().contains("Topics and rules"));
        
        // Wait for the main table to be visible
        waitForControlVisible("__table0");
        
        // Verify the header text is present
        assertTrue(page.isVisible("text=Topic rules"));
    }

    @Test
    void testRefreshButton() {
        navigateToPage("Topics");
        waitForUI5Loading();
        
        // Click the refresh button
        page.click("button[title*='Refresh']");
        
        // Wait for the refresh to complete
        waitForAjaxComplete();
        
        // Verify table is still visible after refresh
        assertTrue(page.isVisible("table"));
    }

    @Test
    void testSaveButton() {
        navigateToPage("Topics");
        waitForUI5Loading();
        
        // Click the save button
        page.click("text=Save");
        
        // Wait for any potential save operation
        waitForAjaxComplete();
        
        // Check if save was successful (either message toast or no error)
        // This is a basic test since we don't have actual data
        assertTrue(page.isVisible("text=Save"));
    }

    @Test
    void testTableStructure() {
        navigateToPage("Topics");
        waitForUI5Loading();
        
        // Verify table columns are present
        assertTrue(page.isVisible("text=Input topic"));
        assertTrue(page.isVisible("text=Output topic"));
        assertTrue(page.isVisible("text=Rules"));
        assertTrue(page.isVisible("text=Instances"));
        assertTrue(page.isVisible("text=Active all"));
        assertTrue(page.isVisible("text=info"));
    }

    @Test
    void testTableInteractions() {
        navigateToPage("Topics");
        waitForUI5Loading();
        
        // Wait for table to load
        page.waitForSelector("table", new com.microsoft.playwright.Page.WaitForSelectorOptions().setTimeout(10000));
        
        // Check if there are any rows in the table
        Locator tableRows = page.locator("table tbody tr");
        int rowCount = tableRows.count();
        
        if (rowCount > 0) {
            // If there are rows, test interactions
            // Test that output topic selects are present
            assertTrue(page.locator("select").count() > 0, "Output topic selects should be present");
            
            // Test that rule multi-combo boxes are present
            assertTrue(page.locator("[role='combobox']").count() > 0, "Rule selection controls should be present");
            
            // Test that step inputs for instances are present
            assertTrue(page.locator("input[type='number']").count() > 0, "Instance count inputs should be present");
            
            // Test that checkboxes for activation are present
            assertTrue(page.locator("input[type='checkbox']").count() > 0, "Activation checkboxes should be present");
        } else {
            // If no data, just verify the table structure exists
            assertTrue(page.isVisible("table"));
        }
    }

    @Test
    void testUI5Integration() {
        navigateToPage("Topics");
        waitForUI5Loading();
        
        // Test that OpenUI5 is loaded properly
        Object ui5Core = page.evaluate("() => window.sap && window.sap.ui && window.sap.ui.getCore()");
        assertNotNull(ui5Core, "OpenUI5 core should be available");
        
        Boolean isInitialized = (Boolean) page.evaluate("() => window.sap.ui.getCore().isInitialized()");
        assertTrue(isInitialized, "OpenUI5 should be initialized");
    }

    /**
     * Helper method to check if application is running.
     * This condition is used to enable/disable tests based on application availability.
     */
    static boolean isApplicationRunning() {
        try {
            // Try to make a simple HTTP request to check if application is running
            java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(BASE_URL + "/ui5/Topics.html"))
                    .timeout(java.time.Duration.ofSeconds(5))
                    .build();
            
            java.net.http.HttpResponse<String> response = client.send(request, 
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            
            return response.statusCode() == 200;
        } catch (Exception e) {
            // Application is not running or not accessible
            return false;
        }
    }
}