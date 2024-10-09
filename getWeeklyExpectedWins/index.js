const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');
const { getCurrentNFLWeek } = require('../utils'); // Import from utils.js

module.exports = async function (context, req) {
    context.log('Processing request for weekly expected wins...');

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

        // Get the week from the request parameters or calculate the current week
        let week = req.query.week ? parseInt(req.query.week) : getCurrentNFLWeek();
        context.log(`Using NFL Week: ${week}`);

        // Query to calculate expected wins
        const query = `
            SELECT
                u.UserID,
                u.UserName,
                ROUND(SUM(ExpectedWin), 2) AS TotalExpectedWins
            FROM
                (
                    SELECT
                        ut.UserID,
                        ut.TeamID,
                        CASE
                            WHEN gl.HomeTeamAbbreviation = t.Abbreviation THEN
                                CASE
                                    WHEN gl.MoneyLineHome < 0 THEN
                                        CAST((-gl.MoneyLineHome) AS FLOAT) / ((-gl.MoneyLineHome) + 100)
                                    ELSE
                                        100.0 / (gl.MoneyLineHome + 100)
                                END
                            WHEN gl.AwayTeamAbbreviation = t.Abbreviation THEN
                                CASE
                                    WHEN gl.MoneyLineAway < 0 THEN
                                        CAST((-gl.MoneyLineAway) AS FLOAT) / ((-gl.MoneyLineAway) + 100)
                                    ELSE
                                        100.0 / (gl.MoneyLineAway + 100)
                                END
                            ELSE 0
                        END AS ExpectedWin
                    FROM
                        UserTeams ut
                    JOIN
                        Teams t ON ut.TeamID = t.TeamID
                    JOIN
                        GameLines gl ON (gl.HomeTeamAbbreviation = t.Abbreviation OR gl.AwayTeamAbbreviation = t.Abbreviation)
                    WHERE
                        gl.Week = @week
                ) AS ExpectedWins
            JOIN
                Users u ON ExpectedWins.UserID = u.UserID
            GROUP BY
                u.UserID,
                u.UserName
            ORDER BY
                TotalExpectedWins DESC;
        `;

        const request = new sql.Request();
        request.input('week', sql.Int, week);

        const result = await request.query(query);
        context.log(`Weekly expected wins data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: result.recordset,
        };
    } catch (err) {
        context.log.error(`Error fetching weekly expected wins: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error fetching weekly expected wins: ${err.message}`,
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