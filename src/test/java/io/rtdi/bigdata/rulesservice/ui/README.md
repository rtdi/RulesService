# UI Test Automation for RulesService

This directory contains UI integration tests for the RulesService application using Microsoft Playwright for Java.

## Overview

The UI tests are designed to automatically test the web interface of the RulesService application, which is built using SAP OpenUI5. The tests cover the main pages and functionality of the application.

## Test Structure

### Base Test Class
- `BaseUITest.java` - Provides common setup and utility methods for all UI tests
  - Browser management (Chromium in headless mode)
  - OpenUI5-specific waiting and interaction methods
  - Common navigation and assertion helpers

### Test Classes
- `TopicsUITest.java` - Tests for the Topics page (Topics.html)
  - Topic rules viewing and management
  - Table interactions and data validation
  - Save and refresh functionality

- `RulesUITest.java` - Tests for the Rules page (Rules.html)
  - Subject and rule browsing
  - Responsive splitter layout
  - Rule selection and navigation

- `SampleUITest.java` - Tests for the Sample page (Sample.html)
  - Sample data collection workflow
  - Topic selection and execution
  - Table data management and saving

## Prerequisites

1. **Application Running**: The tests require the RulesService application to be running on `http://localhost:8080`
2. **Playwright Dependencies**: Playwright for Java is included in the Maven test dependencies
3. **Java 21**: Tests are compatible with Java 21 as per project requirements

## Running the Tests

### Prerequisites Check
The tests include a conditional execution check (`@EnabledIf("isApplicationRunning")`) that automatically disables tests if the application is not accessible.

### Maven Commands
```bash
# Run all tests (including UI tests if application is running)
mvn test

# Run only UI tests
mvn test -Dtest="*UITest"

# Run a specific UI test class
mvn test -Dtest="TopicsUITest"
```

### Test Configuration
- **Browser**: Chromium (headless mode for CI/CD)
- **Timeouts**: 30 seconds for page loads and element waits
- **Base URL**: `http://localhost:8080` (configurable in BaseUITest)

## Test Features

### OpenUI5 Integration
- Waits for OpenUI5 core initialization
- Handles UI5-specific controls and selectors
- Tests UI5 controller availability and functionality

### Responsive Design Testing
- Tests multiple viewport sizes
- Validates layout adaptation
- Ensures functionality across screen sizes

### Error Handling
- Graceful handling of missing data
- Conditional test execution based on data availability
- Robust waiting strategies for dynamic content

### Interaction Testing
- Form inputs and controls
- Button clicks and navigation
- Table operations and selections
- Multi-select and dropdown interactions

## Configuration

### Changing Base URL
To test against a different environment, modify the `BASE_URL` constant in `BaseUITest.java`:

```java
protected static final String BASE_URL = "http://your-server:port";
```

### Browser Configuration
To change browser settings (e.g., run in non-headless mode for debugging):

```java
browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
```

## CI/CD Integration

The tests are designed to run in CI/CD environments:
- Headless browser mode by default
- Conditional execution prevents failures when application is not available
- Standard JUnit 5 integration for reporting

## Troubleshooting

### Tests Skipped
If tests are being skipped, verify:
1. Application is running on the expected URL
2. Application responds with HTTP 200 to basic requests
3. Network connectivity to the application

### Timeouts
If tests are timing out:
1. Check application performance
2. Adjust timeout values in `BaseUITest.java`
3. Verify OpenUI5 initialization is completing

### Element Not Found
If element selection fails:
1. Use browser developer tools to verify selectors
2. Check if UI5 control IDs have changed
3. Ensure proper wait conditions before interactions

## Adding New Tests

To add tests for additional pages:

1. Create a new test class extending `BaseUITest`
2. Add the `@EnabledIf("isApplicationRunning")` annotation
3. Implement page-specific test methods
4. Follow the naming convention `*UITest.java`

Example:
```java
@EnabledIf("isApplicationRunning")
public class NewPageUITest extends BaseUITest {
    
    @Test
    void testNewPageLoads() {
        navigateToPage("NewPage");
        // Add your test logic
    }
}
```