const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Calculating theoretical perfect draft wins for each user...');

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

        const result = await sql.query`
            SELECT dp.PickPosition, dp.UserID, u.UserName, s.TeamID, t.Name AS TeamName, s.OverallRank, s.Wins
            FROM DraftPicks dp
            JOIN Users u ON dp.UserID = u.UserID
            JOIN Standings s ON dp.TeamID = s.TeamID
            JOIN Teams t ON s.TeamID = t.TeamID;
        `;

        const draftResults = result.recordset;
        const userDetails = {};
        const assignedTeams = new Set();

        draftResults.sort((a, b) => a.OverallRank - b.OverallRank);
        const users = Array.from(new Set(draftResults.map(item => ({ UserID: item.UserID, UserName: item.UserName, PickPosition: item.PickPosition })))).sort((a, b) => a.PickPosition - b.PickPosition);

        users.forEach(user => {
            for (let team of draftResults) {
                if (!assignedTeams.has(team.TeamID)) {
                    if (!userDetails[user.UserID]) {
                        userDetails[user.UserID] = {
                            userName: user.UserName,
                            teams: [],
                            totalWins: 0
                        };
                    }
                    userDetails[user.UserID].teams.push({ teamName: team.TeamName, wins: team.Wins });
                    userDetails[user.UserID].totalWins += team.Wins;
                    assignedTeams.add(team.TeamID);
                    break;
                }
            }
        });

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: userDetails
        };
    } catch (err) {
        context.log.error(`Error calculating theoretical perfect draft wins: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error calculating theoretical perfect draft wins: ${err.message}`
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