package io.rtdi.bigdata.rulesservice.ui;

import com.microsoft.playwright.Locator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.*;

/**
 * UI integration tests for the Rules page.
 * Tests the functionality of viewing and managing subjects and rules.
 */
@EnabledIf("isApplicationRunning")
public class RulesUITest extends BaseUITest {

    @Test
    void testRulesPageLoads() {
        navigateToPage("Rules");
        
        // Verify the page title is correct
        assertTrue(page.title().contains("Rules and Rule Groups"));
        
        // Verify the header text is present
        assertTrue(page.isVisible("text=Rulesservice"));
        
        // Wait for the responsive splitter to be visible
        page.waitForSelector("[role='separator']", new com.microsoft.playwright.Page.WaitForSelectorOptions().setTimeout(10000));
    }

    @Test
    void testRefreshButton() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Click the refresh button
        page.click("button[title*='Refresh']");
        
        // Wait for the refresh to complete
        waitForAjaxComplete();
        
        // Verify the subjects list is still visible after refresh
        assertTrue(page.isVisible("text=Select a subject from the schema registry"));
    }

    @Test
    void testSplitterLayout() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Verify both panes are present
        assertTrue(page.isVisible("text=Select a subject from the schema registry"));
        assertTrue(page.isVisible("text=Select a Rule to edit"));
        
        // Verify the splitter structure
        assertTrue(page.locator("[role='separator']").count() > 0, "Splitter should be present");
    }

    @Test
    void testSubjectsList() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Check for subjects list
        assertTrue(page.isVisible("text=Select a subject from the schema registry"));
        
        // Look for list items
        Locator subjectsList = page.locator("ul").first();
        assertTrue(subjectsList.isVisible(), "Subjects list should be visible");
        
        // Check if there are any subjects available
        Locator listItems = page.locator("li[role='option']");
        int itemCount = listItems.count();
        
        if (itemCount > 0) {
            // If subjects are available, test selection
            listItems.first().click();
            waitForAjaxComplete();
            
            // After selecting a subject, rules should load
            // The no-data text should not be visible if rules exist
            page.waitForTimeout(2000); // Wait for potential rules to load
        }
    }

    @Test
    void testRulesListInitialState() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Initially, rules list should show no data message
        assertTrue(page.isVisible("text=Rules are for an input subject, select one"));
    }

    @Test
    void testSubjectRuleInteraction() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Look for subject items
        Locator subjectItems = page.locator("li[role='option']");
        int subjectCount = subjectItems.count();
        
        if (subjectCount > 0) {
            // Click on the first subject
            subjectItems.first().click();
            waitForAjaxComplete();
            
            // Wait for rules to potentially load
            page.waitForTimeout(3000);
            
            // Check if rules loaded or if still showing no-data text
            Locator rulesItems = page.locator("li[role='option']");
            int rulesCount = rulesItems.count();
            
            if (rulesCount > 1) { // More than just the subject
                // If rules are available, test clicking on a rule
                rulesItems.last().click();
                waitForAjaxComplete();
                
                // This should potentially open a new window or navigate
                // For now, just verify the click was processed
                page.waitForTimeout(1000);
            }
        }
    }

    @Test
    void testResponsiveLayout() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Test that the responsive splitter adapts to different viewport sizes
        page.setViewportSize(800, 600);
        waitForUI5Loading();
        
        // Verify layout still works
        assertTrue(page.isVisible("text=Select a subject from the schema registry"));
        assertTrue(page.isVisible("text=Select a Rule to edit"));
        
        // Test with larger viewport
        page.setViewportSize(1200, 800);
        waitForUI5Loading();
        
        // Verify layout still works
        assertTrue(page.isVisible("text=Select a subject from the schema registry"));
        assertTrue(page.isVisible("text=Select a Rule to edit"));
    }

    @Test
    void testUI5Integration() {
        navigateToPage("Rules");
        waitForUI5Loading();
        
        // Test that OpenUI5 is loaded properly
        Object ui5Core = page.evaluate("() => window.sap && window.sap.ui && window.sap.ui.getCore()");
        assertNotNull(ui5Core, "OpenUI5 core should be available");
        
        Boolean isInitialized = (Boolean) page.evaluate("() => window.sap.ui.getCore().isInitialized()");
        assertTrue(isInitialized, "OpenUI5 should be initialized");
        
        // Verify the controller is loaded
        Object controller = page.evaluate("() => window.sap.ui.getCore().byId('__xmlview0') && window.sap.ui.getCore().byId('__xmlview0').getController()");
        assertNotNull(controller, "Rules controller should be available");
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
                    .uri(java.net.URI.create(BASE_URL + "/ui5/Rules.html"))
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