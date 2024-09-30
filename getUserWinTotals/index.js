const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Processing request for user win totals...');

    try {
        // Fetch token using managed identity
        const credential = new DefaultAzureCredential();
        const token = await credential.getToken('https://database.windows.net/');

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
                    token: token.token,
                },
            },
        };

        // Establish SQL connection with retries
        context.log('Attempting to connect to the database...');
        await connectWithRetry(sqlConfig, context);
        context.log('Connected to database successfully.');

        // Query to fetch user win totals
        const result = await sql.query`
            SELECT
                u.UserID,
                u.UserName,
                SUM(s.Wins) AS Wins,
                SUM(s.Losses) AS Losses,
                SUM(s.Ties) AS Ties,
                CAST(SUM(s.Wins) AS FLOAT) / NULLIF(SUM(s.Wins + s.Losses + s.Ties), 0) AS WinPercentage
            FROM
                UserTeams ut
            JOIN
                Users u ON ut.UserID = u.UserID
            JOIN
                Standings s ON ut.TeamID = s.TeamID
            GROUP BY
                u.UserID,
                u.UserName
            ORDER BY
                WinPercentage DESC;
        `;
        context.log(`User win totals data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: result.recordset,
        };
    } catch (err) {
        context.log.error(`Error fetching user win totals: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error fetching user win totals: ${err.message}`,
        };
    } finally {
        await sql.close(); // Ensure the SQL connection is closed
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
