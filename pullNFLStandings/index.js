const axios = require('axios');
const { DefaultAzureCredential } = require('@azure/identity');
const sql = require('mssql');

module.exports = async function (context, myTimer) {
    const standingsDataUrl = 'https://api.mysportsfeeds.com/v2.1/pull/nfl/2024-2025-regular/standings.json';

    try {
        // Fetch standings data from the API
        const response = await axios.get(standingsDataUrl, {
            headers: {
                'Authorization': `Basic ${Buffer.from(process.env.APIKEY + ":MYSPORTSFEEDS").toString("base64")}`
            }
        });

        const teams = response.data.teams;

        if (!teams || teams.length === 0) {
            context.log('No standings data found.');
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

        for (const teamData of teams) {
            const { team, stats, overallRank, conferenceRank, divisionRank, playoffRank } = teamData;

            // Calculate the point differential
            const pointDifferential = stats.standings.pointsFor - stats.standings.pointsAgainst;

            // Construct the query for inserting or updating the Standings table
            const query = `
                MERGE INTO Standings AS target
                USING (VALUES (
                    ${team.id}, ${stats.standings.wins}, ${stats.standings.losses}, ${stats.standings.otWins}, 
                    ${stats.standings.otLosses}, ${stats.standings.winPct}, ${stats.standings.pointsFor}, 
                    ${stats.standings.pointsAgainst}, ${pointDifferential}, '${team.abbreviation}', 
                    '${conferenceRank.conferenceName}', '${divisionRank.divisionName}', ${conferenceRank.rank || 'NULL'}, 
                    ${divisionRank.rank || 'NULL'}, ${stats.standings.ties}, '${team.officialLogoImageSrc}', 
                    '${team.socialMediaAccounts[0]?.value || 'NULL'}', ${overallRank.rank || 'NULL'}, 
                    ${conferenceRank.gamesBack || 'NULL'}, ${playoffRank.rank || 'NULL'}
                )) AS source (
                    TeamID, Wins, Losses, OTWins, OTLosses, WinPct, PointsFor, PointsAgainst, PointDifferential, 
                    Abbreviation, Conference, Division, ConferenceRank, DivisionRank, Ties, OfficialLogoURL, 
                    SocialMedia, OverallRank, GamesBack, PlayoffRank
                )
                ON target.TeamID = source.TeamID
                WHEN MATCHED THEN
                    UPDATE SET 
                        Wins = source.Wins, 
                        Losses = source.Losses, 
                        OTWins = source.OTWins, 
                        OTLosses = source.OTLosses, 
                        WinPct = source.WinPct, 
                        PointsFor = source.PointsFor, 
                        PointsAgainst = source.PointsAgainst, 
                        PointDifferential = source.PointDifferential, 
                        Abbreviation = source.Abbreviation, 
                        Conference = source.Conference, 
                        Division = source.Division, 
                        ConferenceRank = source.ConferenceRank, 
                        DivisionRank = source.DivisionRank, 
                        Ties = source.Ties, 
                        OfficialLogoURL = source.OfficialLogoURL, 
                        SocialMedia = source.SocialMedia, 
                        OverallRank = source.OverallRank, 
                        GamesBack = source.GamesBack, 
                        PlayoffRank = source.PlayoffRank
                WHEN NOT MATCHED THEN
                    INSERT (TeamID, Wins, Losses, OTWins, OTLosses, WinPct, PointsFor, PointsAgainst, PointDifferential, 
                            Abbreviation, Conference, Division, ConferenceRank, DivisionRank, Ties, OfficialLogoURL, 
                            SocialMedia, OverallRank, GamesBack, PlayoffRank)
                    VALUES (
                        source.TeamID, source.Wins, source.Losses, source.OTWins, source.OTLosses, source.WinPct, 
                        source.PointsFor, source.PointsAgainst, source.PointDifferential, source.Abbreviation, 
                        source.Conference, source.Division, source.ConferenceRank, source.DivisionRank, source.Ties, 
                        source.OfficialLogoURL, source.SocialMedia, source.OverallRank, source.GamesBack, source.PlayoffRank
                    );
            `;

            const request = new sql.Request();
            await request.query(query);
            context.log(`Standings data successfully written for TeamID: ${team.id}`);
        }
    } catch (error) {
        context.log('Error fetching or updating standings:', error.message);
    } finally {
        sql.close();
    }
};
