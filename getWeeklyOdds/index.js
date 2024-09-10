const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity'); // Import DefaultAzureCredential

module.exports = async function (context, req) {
    try {
        context.log("Processing request for weekly odds...");

        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: {
                encrypt: true,
                enableArithAbort: true
            },
            authentication: {
                type: 'azure-active-directory-msi-app-service',
                options: {
                    token: await new DefaultAzureCredential().getToken("https://database.windows.net/").token
                }
            }
        };

        await sql.connect(sqlConfig);
        context.log("Connected to database successfully.");

        // Query to get the latest weekly odds from the GameLines table
        const query = `
            SELECT GameLineID, GameID, Week, StartTime, AwayTeamAbbreviation, HomeTeamAbbreviation, 
                   sourceName, moneyLineAway, moneyLineHome, pointSpreadAway, pointSpreadHome, 
                   overUnder, dateFetched
            FROM GameLines
            WHERE Week = @week
            ORDER BY StartTime;
        `;

        // Get the week from the request parameters or default to current week
        const week = req.query.week || 1; // Replace 1 with a function to get the current week if needed

        const request = new sql.Request();
        request.input('week', sql.Int, week);

        const result = await request.query(query);
        context.log(`Weekly odds data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            body: result.recordset
        };
    } catch (err) {
        context.log(`Error fetching or updating weekly odds: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error: ${err.message}`
        };
    } finally {
        sql.close();
    }
};
