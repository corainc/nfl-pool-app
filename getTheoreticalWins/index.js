const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Calculating theoretical total wins for each user...');

    try {
        // Setup database connection
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

        // Fetch the necessary data
        const result = await sql.query`
            SELECT dp.PickPosition, dp.UserID, s.TeamID, s.OverallRank, s.Wins
            FROM DraftPicks dp
            JOIN Standings s ON dp.TeamID = s.TeamID
            ORDER BY s.OverallRank ASC;
        `;

        const draftResults = result.recordset;

        // Draft simulation logic
        const userWins = {};
        const processedTeams = new Set();

        // Sort users by their pick positions
        draftResults.sort((a, b) => a.PickPosition - b.PickPosition);

        draftResults.forEach(({ UserID, TeamID, Wins }) => {
            if (!processedTeams.has(TeamID)) {
                if (!userWins[UserID]) userWins[UserID] = 0;
                userWins[UserID] += Wins;
                processedTeams.add(TeamID);
            }
        });

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: userWins
        };
    } catch (err) {
        context.log.error(`Error calculating theoretical wins: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error calculating theoretical wins: ${err.message}`
        };
    } finally {
        await sql.close();
    }
};

async function connectWithRetry(sqlConfig, context, maxRetries = 5, retryDelay = 5000) {
    let attempt = 0;
    while (attempt < maxRetries) {
        try {
            await sql.connect(sqlConfig);
            context.log(`Database connection established.`);
            return;
        } catch (err) {
            context.log.error(`Database connection attempt ${attempt + 1} failed: ${err.message}`);
            if (attempt < maxRetries - 1) {
                context.log(`Retrying in ${retryDelay / 1000} seconds...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));
            } else {
                throw err;
            }
        }
        attempt++;
    }
}