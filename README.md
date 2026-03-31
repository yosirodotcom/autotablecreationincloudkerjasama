# BigQuery Automator

This Node.js script automates the creation of BigQuery tables by scraping schemas from Google Sheets.

## Prerequisites
1. **Node.js**: You currently do not have Node.js installed on your system. Please download and install it from [nodejs.org](https://nodejs.org/).
2. **Install Dependencies**: Open a terminal in this folder and run:
   ```bash
   npm install
   ```

## Configuration
1. Open the `tables_link.txt` file.
2. Add your table names and Google Sheets URLs, separated by a comma. (e.g., `MyTable, https://docs.google.com/spreadsheets/d/...`)

## Running the Script
Run the script using:
```bash
node index.js
```

## How It Works
1. **Google Login**: The script will open a browser to `accounts.google.com`. Log in manually and then press `Enter` in the terminal to proceed.
2. **Scraping**: It will silently fetch the CSV exports for your Google Sheets and deduce the column data types into a `schemas.json` file.
3. **BigQuery UI**: The script will browse to the BigQuery console. You will be prompted to select your dataset. Once done, press `Enter` in the terminal again to start the UI automation to create the tables.
