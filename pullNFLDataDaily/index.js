// pullNFLDataDaily/index.js

const axios = require('axios');
const { DefaultAzureCredential } = require('@azure/identity');
const sql = require('mssql');

module.exports = async function (context, myTimer) {
    // Get current date in YYYYMMDD format
    const today = new Date();
    const formattedDate = today.toISOString().split('T')[0].replace(/-/g, '');
    const gameDataUrl = `https://api.mysportsfeeds.com/v2.1/pull/nfl/2024-2025-regular/date/${formattedDate}/games.json`;

    try {
        // Fetch game data from API
        context.log(`Fetching game data for date: ${formattedDate}`);
        const response = await axios.get(gameDataUrl, {
            headers: {
                'Authorization': `Basic ${Buffer.from(process.env.APIKEY + ":MYSPORTSFEEDS").toString("base64")}`
            }
        });

        const games = response.data.games;

        if (!games || games.length === 0) {
            context.log('No games data found for today.');
            return;
        }

        // Use Managed Identity (MSI) to connect to SQL Database
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

        // Implement retry logic for database connection
        context.log("Attempting to connect to the database with retries...");
        await connectWithRetry(sqlConfig, context);

        for (const game of games) {
            const { schedule, score } = game;

            // Validate required schedule properties
            if (!schedule || !schedule.id || !schedule.week || !schedule.startTime || !schedule.awayTeam || !schedule.homeTeam || !schedule.venue) {
                context.log('Incomplete game data:', game);
                continue;
            }

            // Prepare data for database insertion
            const startTime = schedule.startTime ? new Date(schedule.startTime).toISOString() : null;
            const endTime = schedule.endTime ? new Date(schedule.endTime).toISOString() : null;
            const originalStartTime = schedule.originalStartTime ? new Date(schedule.originalStartTime).toISOString() : null;

            // Extract scores if available
            const awayScoreTotal = score?.awayScoreTotal ?? null;
            const homeScoreTotal = score?.homeScoreTotal ?? null;

            // Create a new SQL request
            const request = new sql.Request();
            request.input('GameID', sql.Int, schedule.id);
            request.input('Week', sql.Int, schedule.week);
            request.input('StartTime', sql.DateTime, startTime);
            request.input('EndedTime', sql.DateTime, endTime);
            request.input('AwayTeamID', sql.Int, schedule.awayTeam.id);
            request.input('HomeTeamID', sql.Int, schedule.homeTeam.id);
            request.input('VenueID', sql.Int, schedule.venue.id || null);
            request.input('VenueAllegiance', sql.NVarChar(10), schedule.venueAllegiance || null);
            request.input('ScheduleStatus', sql.NVarChar(50), schedule.scheduleStatus || null);
            request.input('OriginalStartTime', sql.DateTime, originalStartTime);
            request.input('DelayedOrPostponedReason', sql.NVarChar(255), schedule.delayedOrPostponedReason || null);
            request.input('PlayedStatus', sql.NVarChar(50), schedule.playedStatus || null);
            request.input('AwayScoreTotal', sql.Int, awayScoreTotal);
            request.input('HomeScoreTotal', sql.Int, homeScoreTotal);

            const query = `
                MERGE INTO Games AS target
                USING (SELECT 
                    @GameID AS GameID, 
                    @Week AS Week, 
                    @StartTime AS StartTime, 
                    @EndedTime AS EndedTime, 
                    @AwayTeamID AS AwayTeamID, 
                    @HomeTeamID AS HomeTeamID, 
                    @VenueID AS VenueID, 
                    @VenueAllegiance AS VenueAllegiance, 
                    @ScheduleStatus AS ScheduleStatus, 
                    @OriginalStartTime AS OriginalStartTime, 
                    @DelayedOrPostponedReason AS DelayedOrPostponedReason, 
                    @PlayedStatus AS PlayedStatus, 
                    @AwayScoreTotal AS AwayScoreTotal, 
                    @HomeScoreTotal AS HomeScoreTotal
                ) AS source
                ON target.GameID = source.GameID
                WHEN MATCHED THEN
                    UPDATE SET 
                        Week = source.Week, 
                        StartTime = source.StartTime, 
                        EndedTime = source.EndedTime,
                        AwayTeamID = source.AwayTeamID, 
                        HomeTeamID = source.HomeTeamID, 
                        VenueID = source.VenueID, 
                        VenueAllegiance = source.VenueAllegiance, 
                        ScheduleStatus = source.ScheduleStatus,
                        OriginalStartTime = source.OriginalStartTime, 
                        DelayedOrPostponedReason = source.DelayedOrPostponedReason, 
                        PlayedStatus = source.PlayedStatus,
                        AwayScoreTotal = source.AwayScoreTotal,
                        HomeScoreTotal = source.HomeScoreTotal
                WHEN NOT MATCHED THEN
                    INSERT (GameID, Week, StartTime, EndedTime, AwayTeamID, HomeTeamID, VenueID, VenueAllegiance, ScheduleStatus, OriginalStartTime, DelayedOrPostponedReason, PlayedStatus, AwayScoreTotal, HomeScoreTotal)
                    VALUES (
                        source.GameID, source.Week, source.StartTime, source.EndedTime, 
                        source.AwayTeamID, source.HomeTeamID, source.VenueID, source.VenueAllegiance, 
                        source.ScheduleStatus, source.OriginalStartTime, source.DelayedOrPostponedReason, source.PlayedStatus, source.AwayScoreTotal, source.HomeScoreTotal
                    );`;

            await request.query(query);
            context.log(`Game and score data successfully written for GameID: ${schedule.id}`);
        }
    } catch (error) {
        context.log.error('Error occurred:', error);
        throw error; // Ensure the function reports failure
    } finally {
        await sql.close();
    }
};

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
