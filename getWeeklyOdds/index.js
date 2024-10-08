const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');
const { getCurrentNFLWeek } = require('../utils'); // Import from utils.js
const moment = require('moment-timezone');

module.exports = async function (context, req) {
    try {
        context.log('Processing request for weekly odds...');

        // Fetch token using managed identity
        const credential = new DefaultAzureCredential();
        const tokenResponse = await credential.getToken('https://database.windows.net/');
        const token = tokenResponse.token;

        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: {
                encrypt: true,
                enableArithAbort: true,
            },
            authentication: {
                type: 'azure-active-directory-access-token',
                options: {
                    token: token,
                },
            },
        };

        // Establish SQL connection with retries
        context.log('Attempting to connect to the database...');
        await connectWithRetry(sqlConfig, context);
        context.log('Connected to database successfully.');

        // Get the week from the request parameters or calculate the current week
        let week = req.query.week ? parseInt(req.query.week) : getCurrentNFLWeek();
        context.log(`Using NFL Week: ${week}`);

        // Updated query to get the latest weekly odds from the GameLines table with team names
        const query = `
            SELECT
                gl.GameLineID,
                gl.GameID,
                gl.Week,
                gl.StartTime,
                gl.AwayTeamAbbreviation,
                awayTeam.Name AS AwayTeamName,
                gl.HomeTeamAbbreviation,
                homeTeam.Name AS HomeTeamName,
                gl.sourceName,
                gl.moneyLineAway,
                gl.moneyLineHome,
                gl.pointSpreadAway,
                gl.pointSpreadHome,
                gl.overUnder,
                gl.dateFetched
            FROM
                GameLines gl
            LEFT JOIN
                Teams awayTeam ON gl.AwayTeamAbbreviation = awayTeam.Abbreviation
            LEFT JOIN
                Teams homeTeam ON gl.HomeTeamAbbreviation = homeTeam.Abbreviation
            WHERE
                gl.Week = @week
            ORDER BY
                gl.StartTime;
        `;

        const request = new sql.Request();
        request.input('week', sql.Int, week);

        const result = await request.query(query);
        context.log(`Weekly odds data retrieved: ${result.recordset.length} records found.`);

        const formattedData = result.recordset.map(record => {
            const startTimeMoment = moment(record.StartTime).tz('America/Chicago');
            const dateFetchedMoment = moment(record.dateFetched).tz('America/Chicago');
            
            return {
                ...record,
                StartDate: startTimeMoment.format('YYYY-MM-DD'),
                StartTime: startTimeMoment.format('HH:mm:ss'),
                DateFetchedDate: dateFetchedMoment.format('YYYY-MM-DD'),
                DateFetchedTime: dateFetchedMoment.format('HH:mm:ss'),
            };
        });

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: formattedData,
        };

    } catch (err) {
        context.log.error(`Error fetching weekly odds: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error fetching weekly odds: ${err.message}`,
        };
    } finally {
        await sql.close();
    }
};

// Function to connect with retries
async function connectWithRetry(sqlConfig, context, maxRetries = 5, retryDelay = 5000) {
    let attempt = 1;
    while (attempt <= maxRetries) {
        try {
            await sql.connect(sqlConfig);
            context.log(`Database connection established on attempt ${attempt}.`);
            return;
        } catch (err) {
            context.log.error(`Database connection attempt ${attempt} failed: ${err.message}`);
            if (attempt < maxRetries) {
                context.log(`Retrying in ${retryDelay / 1000} seconds...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));
            } else {
                context.log.error('Max retries reached. Throwing error.');
                throw err;
            }
        }
        attempt++;
    }
}