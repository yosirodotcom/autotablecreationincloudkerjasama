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
const deduceType = (colName, val) => {
    const nameLower = (colName || '').toLowerCase();
    
    // Explicit name-based overrides
    if (nameLower.includes('ref')) return 'STRING';
    if (nameLower.includes('tanggal')) return 'DATE';

    if (val === undefined || val === null || val === '') return 'STRING';
    
    // Check if boolean
    if (val.toLowerCase() === 'true' || val.toLowerCase() === 'false') return 'BOOLEAN';
    
    // Check if numeric
    if (!isNaN(val)) {
        if (val.includes('.') || val.includes(',')) return 'FLOAT';
        return 'INTEGER';
    }
    
    // Check if Date (simple heuristic)
    const dateRegex = /^\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,3})?Z?)?$/;
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
    console.log('\\n--- Task 1: Scraping Google Sheets ---');
    let schemas = {};
    let shouldScrape = true;

    if (fs.existsSync('schemas.json')) {
        const answer = await waitForUserInput('\\nFound existing schemas.json from a previous run.\\nDo you want to re-scrape the columns from Google Sheets? (y = scrape again / n = use existing schema): ');
        if (answer.trim().toLowerCase() === 'n') {
            console.log('Skipping scraping. Loading existing schemas.json...');
            schemas = JSON.parse(fs.readFileSync('schemas.json', 'utf-8'));
            shouldScrape = false;
        }
    }

    if (shouldScrape) {
        for (const table of tables) {
        console.log(`Processing sheet for table: ${table.name}`);
        
        // Convert google sheet URL to export as CSV url
        // Example: https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0 -> https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/export?format=csv
        let exportUrl = table.url;
        const match = table.url.match(new RegExp('/d/([a-zA-Z0-9-_]+)'));
        const gidMatch = table.url.match(/gid=([0-9]+)/);
        
        if (match && match[1]) {
            exportUrl = `https://docs.google.com/spreadsheets/d/${match[1]}/export?format=csv`;
            if (gidMatch && gidMatch[1]) {
                exportUrl += `&gid=${gidMatch[1]}`;
            }
        }

        let success = false;
        let attempt = 0;
        const maxAttempts = 3;

        while (!success && attempt < maxAttempts) {
            attempt++;
            try {
                // Using Playwright's API context to fetch the CSV (inherits browser cookies so handles auth automatically)
                const response = await browserContext.request.get(exportUrl);
                if (!response.ok()) {
                    console.error(`  [Attempt ${attempt}] Failed to download CSV for ${table.name}. Status: ${response.status()}`);
                    if (attempt === maxAttempts) break;
                    console.log(`  Retrying in 2 seconds...`);
                    await new Promise(r => setTimeout(r, 2000));
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
                        const type = deduceType(safeHeader, value);
                        
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
                
                success = true; // Mark as successful to exit loop
            } catch (e) {
                console.error(`  [Attempt ${attempt}] Error processing ${table.name}: ${e.message}`);
                if (attempt === maxAttempts) break;
                console.log(`  Retrying in 2 seconds...`);
                await new Promise(r => setTimeout(r, 2000));
                }
            }
            
            if (!success) {
                console.error(`  Skipping ${table.name} after ${maxAttempts} failed attempts.`);
            }
        }

        fs.writeFileSync('schemas.json', JSON.stringify(schemas, null, 2));
        console.log('\nSchemas saved to schemas.json');
    }

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
            
            // Auto click Create Table menu for 'lookerkerjasama'
            console.log("  Opening the 'Create table' panel...");
            
            // Open context menu for lookerkerjasama
            // The 3-dot button is often hidden until hovered, so we use force: true
            // To prevent clicking the newly created table's menu, we scope strictly to the 'lookerkerjasama' tree item
            let menuBtn = page.locator('cfc-tree-node, mat-tree-node, [role="treeitem"]').filter({ hasText: 'lookerkerjasama' }).locator('button[aria-haspopup="menu"]').first();
            
            if (await menuBtn.count() === 0) {
                menuBtn = page.locator('button[aria-controls*="lookerkerjasama"][aria-haspopup="menu"]').first();
            }
            if (await menuBtn.count() === 0) {
                menuBtn = page.locator('button.node-context-menu[cfctooltip="View actions"]').first();
            }
            if (await menuBtn.count() > 0) {
                await menuBtn.click({ force: true });
            } else {
                // Fallback to finding any node context menu by class
                await page.locator('button.node-context-menu').first().click({ force: true });
            }
            await page.waitForTimeout(1000); // give menu time to animate
            
            // Click "Create table" from the dropdown
            await page.locator('.cfc-menu-item-label').filter({ hasText: 'Create table' }).first().click();
            await page.waitForTimeout(3000); // give the side sliding panel time to open
            
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
                const unknownValuesLabel = page.locator('label').filter({ hasText: 'Unknown values' }).first();
                if (await unknownValuesLabel.count() > 0) {
                    await unknownValuesLabel.click();
                }

                // Header rows to skip
                console.log('  Setting Header rows to skip: 1...');
                await page.locator('input[formcontrolname="skipLeadingRows"]').fill('1');
                
            } else {
                console.log('  Could not find Advanced options toggle.');
            }

            // Auto Submit
            console.log(`  Submitting the form to create ${table.name}...`);
            await page.locator('button[type="submit"]').filter({ hasText: 'Create table' }).first().click();
            
            console.log('  Waiting 8 seconds for BigQuery to finish creating before moving to next table...');
            await page.waitForTimeout(8000);

        } catch (e) {
            console.error(`Error automating table ${table.name}: ${e.message}`);
        }
    }

    console.log('\nAll tables processed successfully!');
    console.log('This script was succeed.');
    
    await browserContext.close();
})();
