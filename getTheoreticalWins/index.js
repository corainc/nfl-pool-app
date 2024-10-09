const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Calculating theoretical total wins and team assignments for each user...');

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
            SELECT dp.PickPosition, dp.UserID, u.UserName, s.TeamID, t.Name AS TeamName, s.OverallRank, s.Wins
            FROM DraftPicks dp
            JOIN Users u ON dp.UserID = u.UserID
            JOIN Standings s ON dp.TeamID = s.TeamID
            JOIN Teams t ON s.TeamID = t.TeamID
            ORDER BY s.OverallRank ASC;
        `;

        const draftResults = result.recordset;

        // Initialize data structures for user teams and wins
        const userDetails = {};

        // Draft simulation logic
        const processedTeams = new Set();
        
        // Sort users by their pick positions
        draftResults.sort((a, b) => a.PickPosition - b.PickPosition);

        draftResults.forEach(({ UserID, UserName, TeamID, TeamName, Wins }) => {
            if (!processedTeams.has(TeamID)) {
                if (!userDetails[UserID]) {
                    userDetails[UserID] = {
                        userName: UserName,
                        teams: [],
                        totalWins: 0
                    };
                }
                userDetails[UserID].teams.push({ teamName: TeamName, wins: Wins });
                userDetails[UserID].totalWins += Wins;
                processedTeams.add(TeamID);
            }
        });

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: userDetails
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