const axios = require('axios');
const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

// Function to format the odds with a "+" sign for positive values
function formatOdds(value) {
    if (value > 0) {
        return `+${value}`;
    }
    return value; // Return as is for negative or null values
}

// Main function to pull NFL odds and insert into the database
module.exports = async function (context, myTimer) {
    const week = 1; // For simplicity, manually setting to week 1. You may adjust dynamically as needed.
    context.log('NFL Weekly Game Odds Data Triggered!');

    // Step 1: Fetch Data from MySportsFeeds API
    const apiUrl = `https://api.mysportsfeeds.com/v2.1/pull/nfl/2024-2025-regular/week/${week}/odds_gamelines.json?source=bovada`;
    try {
        const response = await axios.get(apiUrl, {
            headers: {
                'Authorization': `Basic ${Buffer.from(process.env.APIKEY + ':MYSPORTSFEEDS').toString('base64')}`
            }
        });
        const data = response.data;
        context.log(`API Response: ${JSON.stringify(data)}`);

        if (!data.gameLines || data.gameLines.length === 0) {
            context.log('No game lines data found.');
            return;
        }

        // Step 2: Connect to SQL Database using Managed Identity
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

        try {
            context.log('Attempting to connect to the database...');
            await sql.connect(sqlConfig);
            context.log('Connected to the database successfully.');

            // Step 3: Process the data and insert into the database
            for (const gameLine of data.gameLines) {
                await insertGameLineIntoDatabase(context, gameLine.game, gameLine);
            }
        } catch (dbError) {
            context.log('Error connecting to the database:', dbError.message || dbError);
        }
    } catch (apiError) {
        context.log('Error fetching data from MySportsFeeds API:', apiError.message || apiError);
    }
};

// Function to insert game line data into the database
async function insertGameLineIntoDatabase(context, game, gameLine) {
    const { startTime, awayTeamAbbreviation, homeTeamAbbreviation } = game;

    context.log("Game ID:", game.id);

    if (!gameLine.lines || gameLine.lines.length === 0) {
        context.log('No lines available for this game');
        return;
    }

    // Get the latest Bovada line for the FULL game segment
    const latestBovadaLine = gameLine.lines
        .filter(line => line.source?.name?.toLowerCase() === 'bovada')
        .map(line => {
            const moneyLine = line.moneyLines?.find(ml => ml.moneyLine.gameSegment === 'FULL');
            const pointSpread = line.pointSpreads?.find(ps => ps.pointSpread.gameSegment === 'FULL');
            const overUnder = line.overUnders?.find(ou => ou.overUnder.gameSegment === 'FULL');

            return {
                moneyLineAway: moneyLine ? formatOdds(moneyLine.moneyLine.awayLine.american) : null,
                moneyLineHome: moneyLine ? formatOdds(moneyLine.moneyLine.homeLine.american) : null,
                pointSpreadAway: pointSpread ? formatOdds(pointSpread.pointSpread.awaySpread) : null,
                pointSpreadHome: pointSpread ? formatOdds(pointSpread.pointSpread.homeSpread) : null,
                overUnder: overUnder ? formatOdds(overUnder.overUnder.overLine.american) : null,
                sourceName: 'Bovada'
            };
        })[0];

    if (latestBovadaLine) {
        context.log(`Inserting GameID: ${game.id}, MoneyLineAway: ${latestBovadaLine.moneyLineAway}, MoneyLineHome: ${latestBovadaLine.moneyLineHome}, PointSpreadAway: ${latestBovadaLine.pointSpreadAway}, PointSpreadHome: ${latestBovadaLine.pointSpreadHome}, OverUnder: ${latestBovadaLine.overUnder}`);

        try {
            const request = new sql.Request();
            await request.query(`
                INSERT INTO GameLines (GameID, Week, StartTime, AwayTeamAbbreviation, HomeTeamAbbreviation, moneyLineAway, moneyLineHome, pointSpreadAway, pointSpreadHome, overUnder, sourceName, dateFetched)
                VALUES (${game.id}, ${game.week}, '${startTime}', '${awayTeamAbbreviation}', '${homeTeamAbbreviation}', '${latestBovadaLine.moneyLineAway}', '${latestBovadaLine.moneyLineHome}', '${latestBovadaLine.pointSpreadAway}', '${latestBovadaLine.pointSpreadHome}', '${latestBovadaLine.overUnder}', '${latestBovadaLine.sourceName}', GETDATE());
            `);
            context.log(`Inserted game line for GameID: ${game.id}, Source: ${latestBovadaLine.sourceName}`);
        } catch (error) {
            context.log('Error inserting game line:', error.message || error);
        }
    } else {
        context.log('No valid Bovada line for FULL game segment found.');
    }
}
