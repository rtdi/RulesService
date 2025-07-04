package io.rtdi.bigdata.rulesservice.ui;

import com.microsoft.playwright.Locator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI integration tests for the Sample page.
 * Tests the functionality of collecting and managing sample data.
 */
@EnabledIf("isApplicationRunning")
public class SampleUITest extends BaseUITest {

    @Test
    void testSamplePageLoads() {
        navigateToPage("Sample");
        
        // Verify the page title is correct
        assertTrue(page.title().contains("Collect Sample Data"));
        
        // Verify the header text is present
        assertTrue(page.isVisible("text=Topics to Sample"));
        
        // Wait for the topic selector to be visible
        waitForControlVisible("topicselector");
    }

    @Test
    void testTopicSelectorPresent() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Verify the topic selector MultiComboBox is present
        assertTrue(page.isVisible("input[role='combobox']"));
        
        // Verify the Execute button is present
        assertTrue(page.isVisible("text=Execute"));
    }

    @Test
    void testExecuteButtonInitialState() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Click Execute without selecting topics
        page.click("text=Execute");
        
        // Should show a message about no topics selected
        // Wait a bit for any potential message
        page.waitForTimeout(2000);
        
        // The button should still be clickable
        assertTrue(page.isVisible("text=Execute"));
    }

    @Test
    void testTableStructure() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Verify table columns are present
        assertTrue(page.isVisible("text=Partition/Offset"));
        assertTrue(page.isVisible("text=Subject"));
        assertTrue(page.isVisible("text=Payload"));
        assertTrue(page.isVisible("text=Filename"));
        assertTrue(page.isVisible("text=Save"));
        
        // Verify the "Save Selected" button is present
        assertTrue(page.isVisible("text=Save Selected"));
    }

    @Test
    void testTopicSelection() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Click on the topic selector to open it
        page.click("input[role='combobox']");
        
        // Wait for dropdown to potentially open
        page.waitForTimeout(1000);
        
        // Check if there are any topics available
        Locator topicOptions = page.locator("li[role='option']");
        int optionCount = topicOptions.count();
        
        if (optionCount > 0) {
            // If topics are available, test selection
            topicOptions.first().click();
            waitForAjaxComplete();
            
            // Now execute should be possible
            page.click("text=Execute");
            waitForAjaxComplete();
            
            // Wait for potential data to load
            page.waitForTimeout(3000);
        }
    }

    @Test
    void testTableInteractions() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Try to select some topics and execute
        page.click("input[role='combobox']");
        page.waitForTimeout(1000);
        
        Locator topicOptions = page.locator("li[role='option']");
        if (topicOptions.count() > 0) {
            topicOptions.first().click();
            page.click("text=Execute");
            waitForAjaxComplete();
            
            // Wait for data to potentially load
            page.waitForTimeout(5000);
            
            // Check if table has data
            Locator tableRows = page.locator("table tbody tr");
            int rowCount = tableRows.count();
            
            if (rowCount > 0) {
                // Test table interactions
                // Check if subject selects are present in rows
                Locator subjectSelects = page.locator("select");
                assertTrue(subjectSelects.count() > 0, "Subject selection dropdowns should be present");
                
                // Check if filename inputs are present
                Locator filenameInputs = page.locator("input[type='text']");
                assertTrue(filenameInputs.count() > 0, "Filename inputs should be present");
                
                // Check if individual save buttons are present
                Locator saveButtons = page.locator("button").locator("text=Save");
                assertTrue(saveButtons.count() > 0, "Individual save buttons should be present");
                
                // Test row selection
                Locator checkboxes = page.locator("input[type='checkbox']");
                if (checkboxes.count() > 0) {
                    checkboxes.first().click();
                    
                    // Test "Save Selected" functionality
                    page.click("text=Save Selected");
                    waitForAjaxComplete();
                }
            }
        }
    }

    @Test
    void testSaveSelectedButton() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Click "Save Selected" without any selection
        page.click("text=Save Selected");
        
        // Should handle gracefully (no error should occur)
        waitForAjaxComplete();
        
        // Button should still be present
        assertTrue(page.isVisible("text=Save Selected"));
    }

    @Test
    void testScrollContainer() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Verify scroll container is present
        Locator scrollContainer = page.locator("[data-sap-ui-scrollable='true']");
        assertTrue(scrollContainer.count() > 0, "Scroll container should be present");
        
        // Verify table is inside scroll container
        assertTrue(page.isVisible("table"));
    }

    @Test
    void testResponsiveDesign() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Test with different viewport sizes
        page.setViewportSize(800, 600);
        waitForUI5Loading();
        
        // Verify essential elements are still visible
        assertTrue(page.isVisible("text=Topics to Sample"));
        assertTrue(page.isVisible("text=Execute"));
        assertTrue(page.isVisible("table"));
        
        // Test with larger viewport
        page.setViewportSize(1200, 800);
        waitForUI5Loading();
        
        // Verify layout still works
        assertTrue(page.isVisible("text=Topics to Sample"));
        assertTrue(page.isVisible("text=Execute"));
        assertTrue(page.isVisible("table"));
    }

    @Test
    void testUI5Integration() {
        navigateToPage("Sample");
        waitForUI5Loading();
        
        // Test that OpenUI5 is loaded properly
        Object ui5Core = page.evaluate("() => window.sap && window.sap.ui && window.sap.ui.getCore()");
        assertNotNull(ui5Core, "OpenUI5 core should be available");
        
        Boolean isInitialized = (Boolean) page.evaluate("() => window.sap.ui.getCore().isInitialized()");
        assertTrue(isInitialized, "OpenUI5 should be initialized");
        
        // Verify the controller is loaded
        Object controller = page.evaluate("() => window.sap.ui.getCore().byId('__xmlview0') && window.sap.ui.getCore().byId('__xmlview0').getController()");
        assertNotNull(controller, "Sample controller should be available");
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
                    .uri(java.net.URI.create(BASE_URL + "/ui5/Sample.html"))
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