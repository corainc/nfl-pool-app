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

        context.log(`API Response for Teams and Stats: ${JSON.stringify(teamResponse.data)}`);

        // Extract the necessary data for each team
        const teamData = teamResponse.data.teamStatsTotals.map(team => {
            const teamInfo = team.team;
            const stats = team.stats;

            return {
                TeamID: teamInfo.id,
                City: teamInfo.city,
                Name: teamInfo.name,
                Abbreviation: teamInfo.abbreviation,
                HomeVenueID: teamInfo.homeVenue?.id || null,
                TeamColorsHex: teamInfo.teamColoursHex.join(', '),  // Combine array into a single string
                SocialMedia: teamInfo.socialMediaAccounts.map(account => `${account.mediaType}: ${account.value}`).join(', '),  // Join social media accounts into one string
                LogoURL: teamInfo.officialLogoImageSrc,
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
                Tackles: stats?.tackles?.tackleTotal || 0,
                Interceptions: stats?.interceptions?.interceptions || 0,
                Fumbles: stats?.fumbles?.fumLost || 0,
                KickoffReturns: stats?.kickoffReturns?.krRet || 0,
                PuntReturns: stats?.puntReturns?.prRet || 0,
                FieldGoalsMade: stats?.fieldGoals?.fgMade || 0,
                FieldGoalsAttempted: stats?.fieldGoals?.fgAtt || 0,
                ExtraPointsMade: stats?.extraPointAttempt?.xpMade || 0,
                ExtraPointsAttempted: stats?.extraPointAttempt?.xpAtt || 0,
                OffensePlays: stats?.offense?.plays || 0,
                OffenseYards: stats?.offense?.yards || 0,
                OffenseAvgYardsPerPlay: stats?.offense?.avgYards || 0,
                TotalTD: stats?.scoring?.totalTD || 0
            };
        });

        context.log(`Extracted Team Data and Stats: ${JSON.stringify(teamData)}`);

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
                type: 'azure-active-directory-msi-app-service',
                options: {
                    token: accessToken.token
                }
            }
        };

        context.log('Attempting to connect to the database...');
        await sql.connect(sqlConfig);
        context.log('Connected to the database successfully.');

        // Insert the team data into the Teams table and team stats into the TeamStats table using MERGE
        const insertPromises = teamData.map(async (team) => {
            const teamQuery = `
                MERGE INTO Teams AS target
                USING (VALUES (@TeamID, @City, @Name, @Abbreviation, @HomeVenueID, @TeamColorsHex, @SocialMedia, @LogoURL)) 
                AS source (TeamID, City, Name, Abbreviation, HomeVenueID, TeamColorsHex, SocialMedia, LogoURL)
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

            const teamStatsQuery = `
                MERGE INTO TeamStats AS target
                USING (VALUES (@TeamID, @GamesPlayed, @Wins, @Losses, @PointsFor, @PointsAgainst, 
                               @PassingAttempts, @PassingCompletions, @PassingYards, @RushingAttempts, @RushingYards, 
                               @ReceivingYards, @Tackles, @Interceptions, @Fumbles, @KickoffReturns, @PuntReturns, 
                               @FieldGoalsMade, @FieldGoalsAttempted, @ExtraPointsMade, @ExtraPointsAttempted, 
                               @OffensePlays, @OffenseYards, @OffenseAvgYardsPerPlay, @TotalTD)) 
                AS source (TeamID, GamesPlayed, Wins, Losses, PointsFor, PointsAgainst, PassingAttempts, PassingCompletions, 
                           PassingYards, RushingAttempts, RushingYards, ReceivingYards, Tackles, Interceptions, Fumbles, 
                           KickoffReturns, PuntReturns, FieldGoalsMade, FieldGoalsAttempted, ExtraPointsMade, ExtraPointsAttempted, 
                           OffensePlays, OffenseYards, OffenseAvgYardsPerPlay, TotalTD)
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

            const request = new sql.Request();
            request.input('TeamID', team.TeamID);
            request.input('City', team.City);
            request.input('Name', team.Name);
            request.input('Abbreviation', team.Abbreviation);
            request.input('HomeVenueID', team.HomeVenueID);
            request.input('TeamColorsHex', team.TeamColorsHex);
            request.input('SocialMedia', team.SocialMedia);
            request.input('LogoURL', team.LogoURL);

            // Execute the insert for Teams
            await request.query(teamQuery);

            // Reuse request for TeamStats
            request.input('GamesPlayed', team.GamesPlayed);
            request.input('Wins', team.Wins);
            request.input('Losses', team.Losses);
            request.input('PointsFor', team.PointsFor);
            request.input('PointsAgainst', team.PointsAgainst);
            request.input('PassingAttempts', team.PassingAttempts);
            request.input('PassingCompletions', team.PassingCompletions);
            request.input('PassingYards', team.PassingYards);
            request.input('RushingAttempts', team.RushingAttempts);
            request.input('RushingYards', team.RushingYards);
            request.input('ReceivingYards', team.ReceivingYards);
            request.input('Tackles', team.Tackles);
            request.input('Interceptions', team.Interceptions);
            request.input('Fumbles', team.Fumbles);
            request.input('KickoffReturns', team.KickoffReturns);
            request.input('PuntReturns', team.PuntReturns);
            request.input('FieldGoalsMade', team.FieldGoalsMade);
            request.input('FieldGoalsAttempted', team.FieldGoalsAttempted);
            request.input('ExtraPointsMade', team.ExtraPointsMade);
            request.input('ExtraPointsAttempted', team.ExtraPointsAttempted);
            request.input('OffensePlays', team.OffensePlays);
            request.input('OffenseYards', team.OffenseYards);
            request.input('OffenseAvgYardsPerPlay', team.OffenseAvgYardsPerPlay);
            request.input('TotalTD', team.TotalTD);

            // Execute the insert for TeamStats
            await request.query(teamStatsQuery);
        });

        await Promise.all(insertPromises);
        context.log('Team data and stats inserted into the database successfully.');
    } catch (error) {
        context.log(`Error occurred: ${error.message}`);
    }
};

