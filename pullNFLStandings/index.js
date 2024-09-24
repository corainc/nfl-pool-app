// pullNFLStandings/index.js

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

        // Use Managed Identity to connect to SQL Database
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

        context.log("Attempting to connect to the database with retries...");
        await connectWithRetry(sqlConfig, context);

        for (const teamData of teams) {
            const { team, stats, overallRank, conferenceRank, divisionRank, playoffRank } = teamData;

            // Calculate the point differential
            const pointDifferential = (stats.standings.pointsFor || 0) - (stats.standings.pointsAgainst || 0);

            // Prepare SQL parameters
            const request = new sql.Request();
            request.input('TeamID', sql.Int, team.id);
            request.input('Wins', sql.Int, stats.standings.wins || 0);
            request.input('Losses', sql.Int, stats.standings.losses || 0);
            request.input('OTWins', sql.Int, stats.standings.otWins || 0);
            request.input('OTLosses', sql.Int, stats.standings.otLosses || 0);
            request.input('WinPct', sql.Decimal(5, 3), stats.standings.winPct || 0);
            request.input('PointsFor', sql.Int, stats.standings.pointsFor || 0);
            request.input('PointsAgainst', sql.Int, stats.standings.pointsAgainst || 0);
            request.input('PointDifferential', sql.Int, pointDifferential);
            request.input('Abbreviation', sql.NVarChar(10), team.abbreviation || null);
            request.input('Conference', sql.NVarChar(50), conferenceRank?.conferenceName || null);
            request.input('Division', sql.NVarChar(50), divisionRank?.divisionName || null);
            request.input('ConferenceRank', sql.Int, conferenceRank?.rank || null);
            request.input('DivisionRank', sql.Int, divisionRank?.rank || null);
            request.input('Ties', sql.Int, stats.standings.ties || 0);
            request.input('OfficialLogoURL', sql.NVarChar(255), team.officialLogoImageSrc || null);
            request.input('SocialMedia', sql.NVarChar(100), team.socialMediaAccounts[0]?.value || null);
            request.input('OverallRank', sql.Int, overallRank?.rank || null);
            request.input('GamesBack', sql.Float, conferenceRank?.gamesBack || 0);
            request.input('PlayoffRank', sql.Int, playoffRank?.rank || null);
            request.input('HomeWins', sql.Int, stats.standings.homeWins || 0);
            request.input('AwayWins', sql.Int, stats.standings.awayWins || 0);
            request.input('Streak', sql.NVarChar(10), stats.standings.streak?.streakType || null);

            const query = `
                MERGE INTO Standings AS target
                USING (SELECT
                    @TeamID AS TeamID, @Wins AS Wins, @Losses AS Losses, @OTWins AS OTWins, @OTLosses AS OTLosses,
                    @WinPct AS WinPct, @PointsFor AS PointsFor, @PointsAgainst AS PointsAgainst, @PointDifferential AS PointDifferential,
                    @Abbreviation AS Abbreviation, @Conference AS Conference, @Division AS Division, @ConferenceRank AS ConferenceRank,
                    @DivisionRank AS DivisionRank, @Ties AS Ties, @OfficialLogoURL AS OfficialLogoURL, @SocialMedia AS SocialMedia,
                    @OverallRank AS OverallRank, @GamesBack AS GamesBack, @PlayoffRank AS PlayoffRank, @HomeWins AS HomeWins,
                    @AwayWins AS AwayWins, @Streak AS Streak
                ) AS source
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
                        PlayoffRank = source.PlayoffRank,
                        HomeWins = source.HomeWins,
                        AwayWins = source.AwayWins,
                        Streak = source.Streak
                WHEN NOT MATCHED THEN
                    INSERT (TeamID, Wins, Losses, OTWins, OTLosses, WinPct, PointsFor, PointsAgainst, PointDifferential,
                            Abbreviation, Conference, Division, ConferenceRank, DivisionRank, Ties, OfficialLogoURL,
                            SocialMedia, OverallRank, GamesBack, PlayoffRank, HomeWins, AwayWins, Streak)
                    VALUES (source.TeamID, source.Wins, source.Losses, source.OTWins, source.OTLosses, source.WinPct,
                            source.PointsFor, source.PointsAgainst, source.PointDifferential, source.Abbreviation,
                            source.Conference, source.Division, source.ConferenceRank, source.DivisionRank, source.Ties,
                            source.OfficialLogoURL, source.SocialMedia, source.OverallRank, source.GamesBack,
                            source.PlayoffRank, source.HomeWins, source.AwayWins, source.Streak);
            `;

            await request.query(query);
            context.log(`Standings data successfully written for TeamID: ${team.id}`);
        }
    } catch (error) {
        context.log.error('Error fetching or updating standings:', error.message);
        throw error;
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
