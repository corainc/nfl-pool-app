const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Processing request for theoretical wins...');

    try {
        const credential = new DefaultAzureCredential();
        const token = await credential.getToken('https://database.windows.net/');

        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: { encrypt: true, enableArithAbort: true },
            authentication: {
                type: 'azure-active-directory-access-token',
                options: { token: token.token }
            }
        };

        await connectWithRetry(sqlConfig, context);
        context.log('Database connected.');

        // Retrieve users and their draft positions
        const draftOrderResult = await sql.query`
            SELECT dp.UserID, u.UserName, dp.PickPosition
            FROM DraftPicks dp
            JOIN Users u ON dp.UserID = u.UserID
            ORDER BY dp.PickPosition;
        `;

        // Retrieve current team standings
        const standingsResult = await sql.query`
            SELECT s.TeamID, t.Name AS TeamName, s.OverallRank, s.Wins
            FROM Standings s
            JOIN Teams t ON s.TeamID = t.TeamID
            ORDER BY s.OverallRank;
        `;

        // Assign top ranked teams to users based on draft order
        const theoreticalTeams = draftOrderResult.recordset.map((user, index) => {
            const team = standingsResult.recordset[index];
            return {
                UserID: user.UserID,
                UserName: user.UserName,
                TeamID: team.TeamID,
                TeamName: team.TeamName,
                OverallRank: team.OverallRank,
                Wins: team.Wins
            };
        });

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: theoreticalTeams
        };
    } catch (err) {
        context.log.error(`Error processing theoretical wins: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error processing theoretical wins: ${err.message}`
        };
    } finally {
        await sql.close();
    }
};

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
                throw err;
            }
        }
        attempt++;
    }
}