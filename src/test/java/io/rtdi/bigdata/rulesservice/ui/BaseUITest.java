package io.rtdi.bigdata.rulesservice.ui;

import com.microsoft.playwright.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for UI integration tests using Playwright.
 * Provides common setup and teardown functionality for UI testing.
 */
public abstract class BaseUITest {

    protected static Playwright playwright;
    protected static Browser browser;
    protected BrowserContext context;
    protected Page page;

    // Base URL for the application - can be overridden in tests
    protected static final String BASE_URL = "http://localhost:8080";

    @BeforeAll
    static void setUpBeforeClass() {
        playwright = Playwright.create();
        // Use headless mode for CI/CD environments
        browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
    }

    @AfterAll
    static void tearDownAfterClass() {
        if (browser != null) {
            browser.close();
        }
        if (playwright != null) {
            playwright.close();
        }
    }

    @BeforeEach
    void setUp() {
        context = browser.newContext();
        page = context.newPage();
        
        // Set timeouts
        page.setDefaultTimeout(30000); // 30 seconds
        page.setDefaultNavigationTimeout(30000); // 30 seconds
    }

    @AfterEach
    void tearDown() {
        if (page != null) {
            page.close();
        }
        if (context != null) {
            context.close();
        }
    }

    /**
     * Wait for OpenUI5 to be fully loaded
     */
    protected void waitForUI5Loading() {
        // Wait for SAP UI5 core to be available
        page.waitForFunction("() => window.sap && window.sap.ui && window.sap.ui.getCore().isInitialized()");
        // Wait for any loading indicators to disappear
        page.waitForTimeout(1000);
    }

    /**
     * Navigate to a specific UI5 page
     * @param pageName the name of the HTML page (e.g., "Topics", "Rules")
     */
    protected void navigateToPage(String pageName) {
        page.navigate(BASE_URL + "/ui5/" + pageName + ".html");
        waitForUI5Loading();
    }

    /**
     * Wait for a specific UI5 control to be visible
     * @param controlId the ID of the UI5 control
     */
    protected void waitForControlVisible(String controlId) {
        page.waitForSelector("[id$='" + controlId + "']", new Page.WaitForSelectorOptions().setState(WaitForSelectorState.VISIBLE));
    }

    /**
     * Click on a UI5 control by its ID
     * @param controlId the ID of the UI5 control
     */
    protected void clickControl(String controlId) {
        page.click("[id$='" + controlId + "']");
    }

    /**
     * Get text from a UI5 control by its ID
     * @param controlId the ID of the UI5 control
     * @return the text content
     */
    protected String getControlText(String controlId) {
        return page.textContent("[id$='" + controlId + "']");
    }

    /**
     * Check if a message toast is displayed
     * @param message the expected message text
     * @return true if the message is displayed
     */
    protected boolean isMessageToastVisible(String message) {
        try {
            page.waitForSelector(".sapMMessageToast", new Page.WaitForSelectorOptions().setTimeout(5000));
            return page.isVisible(".sapMMessageToast");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Wait for any AJAX requests to complete
     */
    protected void waitForAjaxComplete() {
        page.waitForTimeout(2000); // Wait for any pending requests
    }
}