// pullNFLOddsWeekly/index.js

const axios = require('axios');
const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

// Static list of NFL week end dates
const nflWeekEndDates = [
    new Date("2024-09-09"),  // Week 1 ends
    new Date("2024-09-16"),  // Week 2 ends
    new Date("2024-09-23"),  // Week 3 ends
    new Date("2024-09-30"),  // Week 4 ends
    new Date("2024-10-07"),  // Week 5 ends
    new Date("2024-10-14"),  // Week 6 ends
    new Date("2024-10-21"),  // Week 7 ends
    new Date("2024-10-28"),  // Week 8 ends
    new Date("2024-11-04"),  // Week 9 ends
    new Date("2024-11-11"),  // Week 10 ends
    new Date("2024-11-18"),  // Week 11 ends
    new Date("2024-11-25"),  // Week 12 ends
    new Date("2024-12-02"),  // Week 13 ends
    new Date("2024-12-09"),  // Week 14 ends
    new Date("2024-12-16"),  // Week 15 ends
    new Date("2024-12-23"),  // Week 16 ends
    new Date("2024-12-30"),  // Week 17 ends
    new Date("2025-01-06")   // Week 18 ends
];

// Function to parse and convert values to appropriate data types
function parseMoneyLine(value) {
    if (value === null || value === undefined) return null;
    return parseInt(value, 10);
}

function parseFloatValue(value) {
    if (value === null || value === undefined) return null;
    return parseFloat(value);
}

// Function to get the current NFL week dynamically
function getCurrentNFLWeek() {
    const currentDate = new Date();

    // Loop through each week end date to find where the current date fits
    for (let i = 0; i < nflWeekEndDates.length; i++) {
        if (currentDate <= nflWeekEndDates[i]) {
            return i + 1; // Weeks are 1-based
        }
    }

    return 18; // Default to week 18 if past all weeks
}

// Main function to pull NFL odds and insert into the database
module.exports = async function (context, myTimer) {
    const currentNFLWeek = getCurrentNFLWeek();
    context.log(`NFL Weekly Game Odds Data Triggered for Week ${currentNFLWeek}!`);

    // Step 1: Fetch Data from MySportsFeeds API
    const apiUrl = `https://api.mysportsfeeds.com/v2.1/pull/nfl/2024-2025-regular/week/${currentNFLWeek}/odds_gamelines.json?source=bovada`;

    try {
        const response = await axios.get(apiUrl, {
            headers: {
                'Authorization': `Basic ${Buffer.from(process.env.APIKEY + ':MYSPORTSFEEDS').toString('base64')}`
            }
        });
        const data = response.data;

        if (!data.gameLines || data.gameLines.length === 0) {
            context.log('No game lines data found for the current week.');
            return;
        }

        context.log(`Fetched ${data.gameLines.length} game lines from the API.`);

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
                type: 'azure-active-directory-access-token',
                options: {
                    token: accessToken.token
                }
            }
        };

        try {
            context.log('Attempting to connect to the database with retries...');
            await connectWithRetry(sqlConfig, context);

            // Step 3: Process the data and insert into the database
            for (const gameLine of data.gameLines) {
                await insertGameLineIntoDatabase(context, gameLine.game, gameLine);
            }
        } catch (dbError) {
            context.log.error('Error connecting to the database:', dbError.message || dbError);
            throw dbError;
        } finally {
            await sql.close();
        }
    } catch (apiError) {
        context.log.error('Error fetching data from MySportsFeeds API:', apiError.message || apiError);
        throw apiError;
    }
};

// Function to insert game line data into the database
async function insertGameLineIntoDatabase(context, game, gameLine) {
    const { id: GameID, startTime, awayTeamAbbreviation, homeTeamAbbreviation, week } = game;

    if (!GameID || !startTime || !awayTeamAbbreviation || !homeTeamAbbreviation) {
        context.log('Incomplete game data:', game);
        return;
    }

    if (!gameLine.lines || gameLine.lines.length === 0) {
        context.log('No lines available for this game');
        return;
    }

    // Get the Bovada line for the FULL game segment
    const bovadaLines = gameLine.lines.filter(line => line.source?.name?.toLowerCase() === 'bovada');
    if (!bovadaLines.length) {
        context.log('No Bovada lines found for this game');
        return;
    }

    const latestBovadaLine = bovadaLines[0]; // Assuming the first one is the latest

    const moneyLine = latestBovadaLine.moneyLines?.find(ml => ml.moneyLine.gameSegment === 'FULL');
    const pointSpread = latestBovadaLine.pointSpreads?.find(ps => ps.pointSpread.gameSegment === 'FULL');
    const overUnder = latestBovadaLine.overUnders?.find(ou => ou.overUnder.gameSegment === 'FULL');

    const lineData = {
        moneyLineAway: parseMoneyLine(moneyLine?.moneyLine?.awayLine?.american),
        moneyLineHome: parseMoneyLine(moneyLine?.moneyLine?.homeLine?.american),
        pointSpreadAway: parseFloatValue(pointSpread?.pointSpread?.awaySpread),
        pointSpreadHome: parseFloatValue(pointSpread?.pointSpread?.homeSpread),
        overUnder: parseFloatValue(overUnder?.overUnder?.total),
        sourceName: 'Bovada'
    };

    context.log(`Inserting GameID: ${GameID}, MoneyLineAway: ${lineData.moneyLineAway}, MoneyLineHome: ${lineData.moneyLineHome}, PointSpreadAway: ${lineData.pointSpreadAway}, PointSpreadHome: ${lineData.pointSpreadHome}, OverUnder: ${lineData.overUnder}`);

    try {
        const request = new sql.Request();
        request.input('GameID', sql.Int, GameID);
        request.input('Week', sql.Int, week);
        request.input('StartTime', sql.DateTime, new Date(startTime));
        request.input('AwayTeamAbbreviation', sql.NVarChar(5), awayTeamAbbreviation);
        request.input('HomeTeamAbbreviation', sql.NVarChar(5), homeTeamAbbreviation);
        request.input('moneyLineAway', sql.Int, lineData.moneyLineAway);
        request.input('moneyLineHome', sql.Int, lineData.moneyLineHome);
        request.input('pointSpreadAway', sql.Float, lineData.pointSpreadAway);
        request.input('pointSpreadHome', sql.Float, lineData.pointSpreadHome);
        request.input('overUnder', sql.Float, lineData.overUnder);
        request.input('sourceName', sql.NVarChar(100), lineData.sourceName);

        const query = `
            MERGE INTO GameLines AS target
            USING (SELECT 
                @GameID AS GameID, 
                @Week AS Week, 
                @StartTime AS StartTime, 
                @AwayTeamAbbreviation AS AwayTeamAbbreviation, 
                @HomeTeamAbbreviation AS HomeTeamAbbreviation, 
                @moneyLineAway AS moneyLineAway, 
                @moneyLineHome AS moneyLineHome, 
                @pointSpreadAway AS pointSpreadAway, 
                @pointSpreadHome AS pointSpreadHome, 
                @overUnder AS overUnder, 
                @sourceName AS sourceName, 
                GETDATE() AS dateFetched
            ) AS source
            ON target.GameID = source.GameID AND target.sourceName = source.sourceName
            WHEN MATCHED THEN
                UPDATE SET
                    Week = source.Week,
                    StartTime = source.StartTime,
                    AwayTeamAbbreviation = source.AwayTeamAbbreviation,
                    HomeTeamAbbreviation = source.HomeTeamAbbreviation,
                    moneyLineAway = source.moneyLineAway,
                    moneyLineHome = source.moneyLineHome,
                    pointSpreadAway = source.pointSpreadAway,
                    pointSpreadHome = source.pointSpreadHome,
                    overUnder = source.overUnder,
                    dateFetched = source.dateFetched
            WHEN NOT MATCHED THEN
                INSERT (
                    GameID, Week, StartTime, AwayTeamAbbreviation, HomeTeamAbbreviation,
                    moneyLineAway, moneyLineHome, pointSpreadAway, pointSpreadHome, overUnder,
                    sourceName, dateFetched
                )
                VALUES (
                    source.GameID, source.Week, source.StartTime, source.AwayTeamAbbreviation, source.HomeTeamAbbreviation,
                    source.moneyLineAway, source.moneyLineHome, source.pointSpreadAway, source.pointSpreadHome, source.overUnder,
                    source.sourceName, source.dateFetched
                );
        `;

        await request.query(query);
        context.log(`Inserted/Updated game line for GameID: ${GameID}, Source: ${lineData.sourceName}`);
    } catch (error) {
        context.log.error('Error inserting game line:', error.message || error);
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
