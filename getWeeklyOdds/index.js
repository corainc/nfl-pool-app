const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity'); // Import DefaultAzureCredential

module.exports = async function (context, req) {
    try {
        context.log('Processing request for weekly odds...');

        // Fetch token using managed identity
        const credential = new DefaultAzureCredential();
        const tokenResponse = await credential.getToken('https://database.windows.net/');
        const token = tokenResponse.token;

        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: {
                encrypt: true,
                enableArithAbort: true,
            },
            authentication: {
                type: 'azure-active-directory-access-token',
                options: {
                    token: token,
                },
            },
        };

        // Establish SQL connection with retries
        context.log('Attempting to connect to the database...');
        await connectWithRetry(sqlConfig, context);
        context.log('Connected to database successfully.');

        // Get the week from the request parameters or calculate the current week
        let week = req.query.week ? parseInt(req.query.week) : getCurrentNFLWeek();
        context.log(`Using NFL Week: ${week}`);

        // Query to get the latest weekly odds from the GameLines table
        const query = `
            SELECT GameLineID, GameID, Week, StartTime, AwayTeamAbbreviation, HomeTeamAbbreviation, 
                   sourceName, moneyLineAway, moneyLineHome, pointSpreadAway, pointSpreadHome, 
                   overUnder, dateFetched
            FROM GameLines
            WHERE Week = @week
            ORDER BY StartTime;
        `;

        const request = new sql.Request();
        request.input('week', sql.Int, week);

        const result = await request.query(query);
        context.log(`Weekly odds data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: result.recordset,
        };
    } catch (err) {
        context.log.error(`Error fetching weekly odds: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error fetching weekly odds: ${err.message}`,
        };
    } finally {
        await sql.close();
    }
};

// Function to connect with retries
async function connectWithRetry(sqlConfig, context, maxRetries = 5, retryDelay = 5000) {
    let attempt = 1;
    while (attempt <= maxRetries) {
        try {
            await sql.connect(sqlConfig);
            context.log(`Database connection established on attempt ${attempt}.`);
            return;
        } catch (err) {
            context.log.error(`Database connection attempt ${attempt} failed: ${err.message}`);
            if (attempt < maxRetries) {
                context.log(`Retrying in ${retryDelay / 1000} seconds...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));
            } else {
                context.log.error('Max retries reached. Throwing error.');
                throw err;
            }
        }
        attempt++;
    }
}

// Function to calculate current NFL week
function getCurrentNFLWeek() {
    const seasonStartDate = new Date('2024-09-05T00:00:00Z'); // Adjust this date to the actual season start date
    const today = new Date();
    const oneWeekInMillis = 7 * 24 * 60 * 60 * 1000; // milliseconds in one week

    let weekNumber = Math.floor((today - seasonStartDate) / oneWeekInMillis) + 1;

    // Ensure weekNumber is within the valid range
    if (weekNumber < 1) weekNumber = 1;
    if (weekNumber > 18) weekNumber = 18; // Adjust if the season has more weeks

    return weekNumber;
}
