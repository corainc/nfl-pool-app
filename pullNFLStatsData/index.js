// pullNFLStatsData/index.js

const axios = require('axios');
const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, myTimer) {
    try {
        // Fetch data from MySportsFeeds API for Team Data and Team Stats
        const teamResponse = await axios.get(`https://api.mysportsfeeds.com/v2.1/pull/nfl/2024-2025-regular/team_stats_totals.json`, {
            headers: {
                'Authorization': `Basic ${Buffer.from(process.env.APIKEY + ':MYSPORTSFEEDS').toString('base64')}`
            }
        });

        const teamStatsTotals = teamResponse.data.teamStatsTotals;

        if (!teamStatsTotals || teamStatsTotals.length === 0) {
            context.log('No team stats data found.');
            return;
        }

        // Connect to SQL Database
        const credential = new DefaultAzureCredential();
        const accessToken = await credential.getToken("https://database.windows.net/");
        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: {
                encrypt: true,
                enableArithAbort: true
            },
            authentication: {
                type: 'azure-active-directory-access-token',
                options: {
                    token: accessToken.token
                }
            }
        };

        context.log('Attempting to connect to the database with retries...');
        await connectWithRetry(sqlConfig, context);

        for (const team of teamStatsTotals) {
            const teamInfo = team.team;
            const stats = team.stats;

            // Prepare team data
            const teamData = {
                TeamID: teamInfo.id,
                City: teamInfo.city || null,
                Name: teamInfo.name || null,
                Abbreviation: teamInfo.abbreviation || null,
                HomeVenueID: teamInfo.homeVenue?.id || null,
                TeamColorsHex: teamInfo.teamColoursHex ? teamInfo.teamColoursHex.join(', ') : null,
                SocialMedia: teamInfo.socialMediaAccounts ? teamInfo.socialMediaAccounts.map(account => `${account.mediaType}: ${account.value}`).join(', ') : null,
                LogoURL: teamInfo.officialLogoImageSrc || null
            };

            // Prepare team stats data
            const teamStats = {
                TeamID: teamInfo.id,
                GamesPlayed: stats?.gamesPlayed || 0,
                Wins: stats?.standings?.wins || 0,
                Losses: stats?.standings?.losses || 0,
                PointsFor: stats?.scoring?.pointsFor || 0,
                PointsAgainst: stats?.scoring?.pointsAgainst || 0,
                PassingAttempts: stats?.passing?.passAttempts || 0,
                PassingCompletions: stats?.passing?.passCompletions || 0,
                PassingYards: stats?.passing?.passNetYards || 0,
                RushingAttempts: stats?.rushing?.rushAttempts || 0,
                RushingYards: stats?.rushing?.rushYards || 0,
                ReceivingYards: stats?.receiving?.recYards || 0,
                Tackles: stats?.defense?.tackleSolo || 0,
                Interceptions: stats?.defense?.interceptions || 0,
                Fumbles: stats?.fumbles?.fumLost || 0,
                KickoffReturns: stats?.kickoffReturns?.krRet || 0,
                PuntReturns: stats?.puntReturns?.prRet || 0,
                FieldGoalsMade: stats?.fieldGoals?.fgMade || 0,
                FieldGoalsAttempted: stats?.fieldGoals?.fgAtt || 0,
                ExtraPointsMade: stats?.extraPoints?.xpMade || 0,
                ExtraPointsAttempted: stats?.extraPoints?.xpAtt || 0,
                OffensePlays: stats?.offense?.plays || 0,
                OffenseYards: stats?.offense?.yards || 0,
                OffenseAvgYardsPerPlay: stats?.offense?.avgYards || 0.0,
                TotalTD: stats?.scoring?.tds || 0
            };

            // Insert or update Teams table
            await insertOrUpdateTeam(context, teamData);

            // Insert or update TeamStats table
            await insertOrUpdateTeamStats(context, teamStats);
        }

        context.log('Team data and stats inserted into the database successfully.');
    } catch (error) {
        context.log.error(`Error occurred: ${error.message}`);
        throw error;
    } finally {
        await sql.close();
    }
};

// Function to insert or update team data in the Teams table
async function insertOrUpdateTeam(context, team) {
    try {
        const request = new sql.Request();
        request.input('TeamID', sql.Int, team.TeamID);
        request.input('City', sql.NVarChar(100), team.City);
        request.input('Name', sql.NVarChar(100), team.Name);
        request.input('Abbreviation', sql.NVarChar(10), team.Abbreviation);
        request.input('HomeVenueID', sql.Int, team.HomeVenueID);
        request.input('TeamColorsHex', sql.NVarChar(255), team.TeamColorsHex);
        request.input('SocialMedia', sql.NVarChar(100), team.SocialMedia);
        request.input('LogoURL', sql.NVarChar(255), team.LogoURL);

        const teamQuery = `
            MERGE INTO Teams AS target
            USING (SELECT @TeamID AS TeamID, @City AS City, @Name AS Name, @Abbreviation AS Abbreviation, 
                   @HomeVenueID AS HomeVenueID, @TeamColorsHex AS TeamColorsHex, @SocialMedia AS SocialMedia, 
                   @LogoURL AS LogoURL) AS source
            ON target.TeamID = source.TeamID
            WHEN MATCHED THEN
                UPDATE SET City = source.City, Name = source.Name, Abbreviation = source.Abbreviation, 
                           HomeVenueID = source.HomeVenueID, TeamColorsHex = source.TeamColorsHex, 
                           SocialMedia = source.SocialMedia, LogoURL = source.LogoURL
            WHEN NOT MATCHED THEN
                INSERT (TeamID, City, Name, Abbreviation, HomeVenueID, TeamColorsHex, SocialMedia, LogoURL)
                VALUES (source.TeamID, source.City, source.Name, source.Abbreviation, source.HomeVenueID, 
                        source.TeamColorsHex, source.SocialMedia, source.LogoURL);
        `;

        await request.query(teamQuery);
        context.log(`Team data updated for TeamID: ${team.TeamID}`);
    } catch (error) {
        context.log.error(`Error updating team data for TeamID ${team.TeamID}: ${error.message}`);
        throw error;
    }
}

// Function to insert or update team stats in the TeamStats table
async function insertOrUpdateTeamStats(context, stats) {
    try {
        const request = new sql.Request();
        request.input('TeamID', sql.Int, stats.TeamID);
        request.input('GamesPlayed', sql.Int, stats.GamesPlayed);
        request.input('Wins', sql.Int, stats.Wins);
        request.input('Losses', sql.Int, stats.Losses);
        request.input('PointsFor', sql.Int, stats.PointsFor);
        request.input('PointsAgainst', sql.Int, stats.PointsAgainst);
        request.input('PassingAttempts', sql.Int, stats.PassingAttempts);
        request.input('PassingCompletions', sql.Int, stats.PassingCompletions);
        request.input('PassingYards', sql.Int, stats.PassingYards);
        request.input('RushingAttempts', sql.Int, stats.RushingAttempts);
        request.input('RushingYards', sql.Int, stats.RushingYards);
        request.input('ReceivingYards', sql.Int, stats.ReceivingYards);
        request.input('Tackles', sql.Int, stats.Tackles);
        request.input('Interceptions', sql.Int, stats.Interceptions);
        request.input('Fumbles', sql.Int, stats.Fumbles);
        request.input('KickoffReturns', sql.Int, stats.KickoffReturns);
        request.input('PuntReturns', sql.Int, stats.PuntReturns);
        request.input('FieldGoalsMade', sql.Int, stats.FieldGoalsMade);
        request.input('FieldGoalsAttempted', sql.Int, stats.FieldGoalsAttempted);
        request.input('ExtraPointsMade', sql.Int, stats.ExtraPointsMade);
        request.input('ExtraPointsAttempted', sql.Int, stats.ExtraPointsAttempted);
        request.input('OffensePlays', sql.Int, stats.OffensePlays);
        request.input('OffenseYards', sql.Int, stats.OffenseYards);
        request.input('OffenseAvgYardsPerPlay', sql.Decimal(5, 2), stats.OffenseAvgYardsPerPlay);
        request.input('TotalTD', sql.Int, stats.TotalTD);

        const teamStatsQuery = `
            MERGE INTO TeamStats AS target
            USING (SELECT @TeamID AS TeamID, @GamesPlayed AS GamesPlayed, @Wins AS Wins, @Losses AS Losses, 
                   @PointsFor AS PointsFor, @PointsAgainst AS PointsAgainst, @PassingAttempts AS PassingAttempts, 
                   @PassingCompletions AS PassingCompletions, @PassingYards AS PassingYards, 
                   @RushingAttempts AS RushingAttempts, @RushingYards AS RushingYards, 
                   @ReceivingYards AS ReceivingYards, @Tackles AS Tackles, @Interceptions AS Interceptions, 
                   @Fumbles AS Fumbles, @KickoffReturns AS KickoffReturns, @PuntReturns AS PuntReturns, 
                   @FieldGoalsMade AS FieldGoalsMade, @FieldGoalsAttempted AS FieldGoalsAttempted, 
                   @ExtraPointsMade AS ExtraPointsMade, @ExtraPointsAttempted AS ExtraPointsAttempted, 
                   @OffensePlays AS OffensePlays, @OffenseYards AS OffenseYards, 
                   @OffenseAvgYardsPerPlay AS OffenseAvgYardsPerPlay, @TotalTD AS TotalTD) AS source
            ON target.TeamID = source.TeamID
            WHEN MATCHED THEN
                UPDATE SET GamesPlayed = source.GamesPlayed, Wins = source.Wins, Losses = source.Losses, 
                           PointsFor = source.PointsFor, PointsAgainst = source.PointsAgainst, 
                           PassingAttempts = source.PassingAttempts, PassingCompletions = source.PassingCompletions, 
                           PassingYards = source.PassingYards, RushingAttempts = source.RushingAttempts, 
                           RushingYards = source.RushingYards, ReceivingYards = source.ReceivingYards, 
                           Tackles = source.Tackles, Interceptions = source.Interceptions, Fumbles = source.Fumbles, 
                           KickoffReturns = source.KickoffReturns, PuntReturns = source.PuntReturns, 
                           FieldGoalsMade = source.FieldGoalsMade, FieldGoalsAttempted = source.FieldGoalsAttempted, 
                           ExtraPointsMade = source.ExtraPointsMade, ExtraPointsAttempted = source.ExtraPointsAttempted, 
                           OffensePlays = source.OffensePlays, OffenseYards = source.OffenseYards, 
                           OffenseAvgYardsPerPlay = source.OffenseAvgYardsPerPlay, TotalTD = source.TotalTD
            WHEN NOT MATCHED THEN
                INSERT (TeamID, GamesPlayed, Wins, Losses, PointsFor, PointsAgainst, PassingAttempts, PassingCompletions, 
                        PassingYards, RushingAttempts, RushingYards, ReceivingYards, Tackles, Interceptions, Fumbles, 
                        KickoffReturns, PuntReturns, FieldGoalsMade, FieldGoalsAttempted, ExtraPointsMade, ExtraPointsAttempted, 
                        OffensePlays, OffenseYards, OffenseAvgYardsPerPlay, TotalTD)
                VALUES (source.TeamID, source.GamesPlayed, source.Wins, source.Losses, source.PointsFor, source.PointsAgainst, 
                        source.PassingAttempts, source.PassingCompletions, source.PassingYards, source.RushingAttempts, 
                        source.RushingYards, source.ReceivingYards, source.Tackles, source.Interceptions, source.Fumbles, 
                        source.KickoffReturns, source.PuntReturns, source.FieldGoalsMade, source.FieldGoalsAttempted, 
                        source.ExtraPointsMade, source.ExtraPointsAttempted, source.OffensePlays, source.OffenseYards, 
                        source.OffenseAvgYardsPerPlay, source.TotalTD);
        `;

        await request.query(teamStatsQuery);
        context.log(`Team stats updated for TeamID: ${stats.TeamID}`);
    } catch (error) {
        context.log.error(`Error updating team stats for TeamID ${stats.TeamID}: ${error.message}`);
        throw error;
    }
}

// Function to handle database connection with retries
async function connectWithRetry(config, context, retries = 5, delay = 5000) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            await sql.connect(config);
            context.log('Connected to the database successfully.');
            return;
        } catch (err) {
            context.log(`Database connection attempt ${attempt} failed: ${err.message}`);
            if (attempt < retries) {
                context.log(`Retrying in ${delay / 1000} seconds...`);
                await new Promise(res => setTimeout(res, delay));
            } else {
                throw err;
            }
        }
    }
}
