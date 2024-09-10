const axios = require('axios');
const { DefaultAzureCredential } = require('@azure/identity');
const sql = require('mssql');

module.exports = async function (context, myTimer) {
    const gameDataUrl = 'https://api.mysportsfeeds.com/v2.1/pull/nfl/2024-2025-regular/games.json';

    try {
        // Fetch game data from API
        const response = await axios.get(gameDataUrl, {
            headers: {
                'Authorization': `Basic ${Buffer.from(process.env.APIKEY + ":MYSPORTSFEEDS").toString("base64")}`
            }
        });

        const games = response.data.games;

        if (!games || games.length === 0) {
            context.log('No games data found.');
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
                type: 'azure-active-directory-msi-app-service',
                options: {
                    token: accessToken.token
                }
            }
        };

        context.log("Attempting to connect to the database...");

        await sql.connect(sqlConfig);
        context.log("Connected to the database successfully.");

        for (const game of games) {
            const { schedule, score } = game;  // Added 'score' from the response

            // Check if schedule and its properties are defined
            if (!schedule || !schedule.id || !schedule.week || !schedule.startTime || !schedule.awayTeam || !schedule.homeTeam || !schedule.venue) {
                context.log('Incomplete game data:', game);
                continue;
            }

            // Convert startTime and endTime using native JS function
            const formatDate = (isoDate) => {
                if (!isoDate) return null;
                const date = new Date(isoDate);
                const yyyy = date.getFullYear();
                const MM = (`0${date.getMonth() + 1}`).slice(-2);
                const dd = (`0${date.getDate()}`).slice(-2);
                const hh = (`0${date.getHours()}`).slice(-2);
                const mm = (`0${date.getMinutes()}`).slice(-2);
                const ss = (`0${date.getSeconds()}`).slice(-2);
                return `${yyyy}-${MM}-${dd} ${hh}:${mm}:${ss}`;
            };

            const startTime = formatDate(schedule.startTime);
            const endTime = schedule.endTime ? formatDate(schedule.endTime) : null;

            // Extract scores if available
            const awayScoreTotal = score?.awayScoreTotal || null;
            const homeScoreTotal = score?.homeScoreTotal || null;

            const query = `
                MERGE INTO Games AS target
                USING (VALUES (
                    ${schedule.id}, ${schedule.week}, '${startTime}', ${endTime ? `'${endTime}'` : 'NULL'}, 
                    '${schedule.awayTeam.id}', '${schedule.homeTeam.id}', ${schedule.venue.id || null}, '${schedule.venueAllegiance || null}', 
                    '${schedule.scheduleStatus}', ${schedule.originalStartTime ? `'${formatDate(schedule.originalStartTime)}'` : 'NULL'}, '${schedule.delayedOrPostponedReason || null}',
                    '${schedule.playedStatus}', ${awayScoreTotal || 'NULL'}, ${homeScoreTotal || 'NULL'}
                )) AS source (GameID, Week, StartTime, EndedTime, AwayTeamID, HomeTeamID, VenueID, VenueAllegiance, ScheduleStatus, OriginalStartTime, DelayedOrPostponedReason, PlayedStatus, AwayScoreTotal, HomeScoreTotal)
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
                    );
            `;

            const request = new sql.Request();
            await request.query(query);
            context.log(`Game and score data successfully written for GameID: ${schedule.id}`);
        }
    } catch (error) {
        context.log('Error occurred:', error.message);
    } finally {
        sql.close();
    }
};

