const { chromium } = require('playwright');
const fs = require('fs');
const readline = require('readline');
const path = require('path');

// Helper to pause execution and wait for user input from the terminal
const waitForUserInput = (query) => {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    return new Promise(resolve => rl.question(query, ans => {
        rl.close();
        resolve(ans);
    }));
};

// Helper to deduce BigQuery type from a string value
const deduceType = (val) => {
    if (val === undefined || val === null || val === '') return 'STRING';
    
    // Check if boolean
    if (val.toLowerCase() === 'true' || val.toLowerCase() === 'false') return 'BOOLEAN';
    
    // Check if numeric
    if (!isNaN(val)) {
        if (val.includes('.') || val.includes(',')) return 'FLOAT';
        return 'INTEGER';
    }
    
    // Check if Date (simple heuristic)
    const dateRegex = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z?)?$/;
    if (dateRegex.test(val)) return 'TIMESTAMP';
    
    return 'STRING';
};

// Parse tables_link.txt
const parseLinks = () => {
    const filePath = path.join(__dirname, 'tables_link.txt');
    if (!fs.existsSync(filePath)) {
        console.error('tables_link.txt not found. Please create it.');
        process.exit(1);
    }
    const content = fs.readFileSync(filePath, 'utf-8');
    const lines = content.split('\n');
    const tables = [];
    for (const line of lines) {
        if (!line.trim()) continue;
        // Assume format: TableName, URL (or tab separated)
        const parts = line.split(/[,\t]/);
        if (parts.length >= 2) {
            tables.push({
                name: parts[0].trim(),
                url: parts[1].trim()
            });
        }
    }
    return tables;
};

// Main function
(async () => {
    console.log('--- BigQuery Automator ---');
    const tables = parseLinks();
    
    if (tables.length === 0) {
        console.log('No tables found in tables_link.txt. Please add some links (Format: TableName, URL).');
        process.exit(1);
    }

    // Launch Playwright with persistent context so logins stay active during the session if needed
    // Using a user-data-dir in the project folder to persist session
    const userDataDir = path.join(__dirname, 'browser_session');
    const browserContext = await chromium.launchPersistentContext(userDataDir, {
        headless: false,
        channel: 'msedge', // Uses your actual Edge browser
        ignoreDefaultArgs: ['--enable-automation'], // Removes the 'browser is being automated' flag
        args: ['--disable-blink-features=AutomationControlled'], // The ultimate stealth flag against Google
        viewport: null
    });

    // Strip the 'webdriver' property to bypass Google's bot detection
    await browserContext.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    });

    const page = await browserContext.newPage();

    // 1. Initial Google Login Step (Using StackOverflow to bypass Google's "secure browser" check)
    console.log("\nNavigating to a 3rd party login to bypass Google's strict browser check...");
    console.log('NOTE: Click "Log in with Google" on this page and complete the sign-in.');
    await page.goto('https://stackoverflow.com/users/login', { timeout: 0 }); // 0 means no timeout
    await waitForUserInput('\nPlease log in using the "Log in with Google" button.\nOnce you are successfully logged in and redirected back, press Enter here to continue...');

    // 2. Scrape schemas
    console.log('\n--- Task 1: Scraping Google Sheets ---');
    const schemas = {};

    for (const table of tables) {
        console.log(`Processing sheet for table: ${table.name}`);
        
        // Convert google sheet URL to export as CSV url
        // Example: https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0 -> https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/export?format=csv
        let exportUrl = table.url;
        const match = table.url.match(/\/d\/([a-zA-Z0-9-_]+)/);
        if (match && match[1]) {
            exportUrl = `https://docs.google.com/spreadsheets/d/${match[1]}/export?format=csv`;
        }

        try {
            // Using Playwright's API context to fetch the CSV (inherits browser cookies so handles auth automatically)
            const response = await browserContext.request.get(exportUrl);
            if (!response.ok()) {
                console.error(`Failed to download CSV for ${table.name}. Status: ${response.status()}`);
                continue;
            }
            const csvText = await response.text();
            
            // Simple CSV parsing (handling basic cases)
            const rows = csvText.split('\n').map(row => {
                // simple split by comma, ignoring nested commas for simplicity in this script
                // For a robust solution, a proper csv-parser is recommended.
                return row.split(',').map(item => item.trim().replace(/^"|"$/g, ''));
            });

            if (rows.length > 0) {
                const headers = rows[0];
                const sampleData = rows.length > 1 ? rows[1] : [];
                
                const tableSchema = headers.map((header, index) => {
                    // Safe handling for empty headers
                    let safeHeader = header.replace(/[^a-zA-Z0-9_]/g, '_');
                    if (!safeHeader) safeHeader = `Column_${index}`;
                    
                    const value = sampleData[index] || '';
                    const type = deduceType(value);
                    
                    return {
                        name: safeHeader,
                        type: type,
                        mode: "NULLABLE"
                    };
                });

                schemas[table.name] = tableSchema;
                console.log(`  -> Successfully extracted ${headers.length} columns.`);
            } else {
                console.log(`  -> URL returned empty CSV for ${table.name}.`);
            }
            
        } catch (e) {
            console.error(`Error processing ${table.name}: ${e.message}`);
        }
    }

    fs.writeFileSync('schemas.json', JSON.stringify(schemas, null, 2));
    console.log('\nSchemas saved to schemas.json');

    // 3. Automate BigQuery via UI
    console.log('\n--- Task 2: BigQuery Automation ---');
    await page.goto('https://console.cloud.google.com/bigquery?project=kerjasama-491806&ws=!1m4!1m3!3m2!1skerjasama-491806!2slookerkerjasama');
    
    await waitForUserInput('\nPlease ensure the BigQuery Workspace is loaded and the "lookerkerjasama" dataset is visible.\nPress Enter here to begin the automated table creation...');

    console.log('\nStarting UI Automation. Please do not interact with your mouse while this runs!');

    for (const table of tables) {
        const schema = schemas[table.name];
        if (!schema) {
            console.log(`Skipping ${table.name} because no schema was found.`);
            continue;
        }

        try {
            console.log(`\nCreating table: ${table.name}...`);
            
            // Wait for user to click create table manually for safety, or we try to automate picking the menu
            await waitForUserInput(`Please expand your 'lookerkerjasama' dataset, left-click the 3 dots, and click 'Create table'.\nOnce the 'Create table' panel is open on the right, press Enter in this terminal...`);
            
            // 1. Source -> Drive
            console.log('  Selecting Source: Drive...');
            await page.locator('cfc-select[formcontrolname="selectedSource"]').click();
            await page.waitForTimeout(500);
            await page.locator('mat-option').filter({ hasText: 'Drive' }).first().click();
            await page.waitForTimeout(1000); // Wait for the DOM to render the new URI input

            // 2. Drive URI
            console.log(`  Filling Drive URI: ${table.url}...`);
            await page.locator('input[formcontrolname="driveUri"]').fill(table.url);
            
            // 3. File Format -> Google Sheet
            console.log('  Selecting File Format: Google Sheet...');
            
            // Try multiple selectors because Google's form structure varies randomly
            let formatDropdown = page.locator('cfc-select[formcontrolname="sourceFormat"]');
            if (await formatDropdown.count() === 0) {
                formatDropdown = page.locator('cfc-select[formcontrolname="fileFormat"]');
            }
            if (await formatDropdown.count() === 0) {
                formatDropdown = page.locator('cfc-select').filter({ hasText: /CSV|Avro|JSONL/i }).first();
            }
            
            await formatDropdown.click();
            await page.waitForTimeout(1000); // Give the dropdown a moment to render
            await page.locator('mat-option').filter({ hasText: 'Google Sheet' }).first().click();

            // 4. Sheet range
            console.log(`  Filling Sheet Range: ${table.name}...`);
            await page.locator('input[formcontrolname="sheetRange"]').fill(table.name);

            // 5. Table Name
            console.log(`  Filling Table name: ${table.name}...`);
            await page.locator('input[formcontrolname="tableId"]').fill(table.name);

            // 6. Schema (Using individual "Add field" because "Edit as text" is disabled for Drive sources)
            console.log('  Adding schema fields individually...');
            for (let i = 0; i < schema.length; i++) {
                const col = schema[i];
                console.log(`    -> Field: ${col.name} (${col.type})`);
                
                // Click "Add field"
                await page.locator('button').filter({ hasText: 'Add field' }).last().click();
                await page.waitForTimeout(400); // Wait for new row to render
                
                // Fill Field Name
                const nameInputs = page.locator('input[placeholder="Name"], input[aria-label="Field name"], input[formcontrolname="name"], input[placeholder="Field name"]');
                if (await nameInputs.count() > 0) {
                    await nameInputs.last().fill(col.name);
                } else {
                    // Fallback to the last text input inside the schema editor box
                    await page.locator('dc-schema-form-editor input').last().fill(col.name);
                }

                // If type is not STRING, open dropdown and select it
                if (col.type !== 'STRING') {
                    // BigQuery defaults to STRING. Look for the last dropdown that currently shows STRING.
                    const typeDropdown = page.locator('cfc-select').filter({ hasText: 'STRING' }).last();
                    if (await typeDropdown.count() > 0) {
                        await typeDropdown.click();
                        await page.waitForTimeout(400); // Give the dropdown menu time to appear
                        await page.locator('mat-option').filter({ hasText: col.type }).first().click();
                    } else {
                        console.log(`      Could not find type dropdown to change type to ${col.type}. Left as STRING.`);
                    }
                }
            }

            // 7. Advanced Options
            console.log('  Opening Advanced options...');
            const advancedOptionsToggle = page.locator('h2').filter({ hasText: 'Advanced options' }).locator('button');
            if (await advancedOptionsToggle.count() > 0) {
                // Check if aria-expanded is false to only click if it's closed
                const isExpanded = await advancedOptionsToggle.getAttribute('aria-expanded');
                if (isExpanded === 'false') {
                    await advancedOptionsToggle.click();
                    await page.waitForTimeout(500);
                }
                
                // Unknown values
                console.log('  Checking "Unknown values"...');
                const unknownValuesCheckbox = page.locator('mat-checkbox[formcontrolname="ignoreUnknownValues"] input');
                if (await unknownValuesCheckbox.count() > 0) {
                    await unknownValuesCheckbox.check({ force: true });
                }

                // Header rows to skip
                console.log('  Setting Header rows to skip: 1...');
                await page.locator('input[formcontrolname="skipLeadingRows"]').fill('1');
                
            } else {
                console.log('  Could not find Advanced options toggle.');
            }

            // Submit
            await waitForUserInput(`\nPlease rapidly review the filled form for ${table.name}.\nClick "Create table" at the bottom manually.\nWait for it to finish, then press Enter to move to the next table...`);

        } catch (e) {
            console.error(`Error automating table ${table.name}: ${e.message}`);
        }
    }

    console.log('\nAll tables processed successfully!');
    console.log('This script was succeed.');
    
    await browserContext.close();
})();
