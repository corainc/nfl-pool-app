const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Processing request for draft picks...');

    try {
        // Fetch token using managed identity
        const credential = new DefaultAzureCredential();
        const token = await credential.getToken('https://database.windows.net/');

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
                    token: token.token,
                },
            },
        };

        // Establish SQL connection with retries
        context.log('Attempting to connect to the database...');
        await connectWithRetry(sqlConfig, context);
        context.log('Connected to database successfully.');

        // Query to fetch draft picks
        const result = await sql.query`SELECT * FROM DraftPicks ORDER BY PickNumber ASC`;
        context.log(`Draft picks data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
            body: result.recordset,
        };
    } catch (err) {
        context.log.error(`Error fetching draft picks: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error fetching draft picks: ${err.message}`,
        };
    } finally {
        await sql.close(); // Ensure the SQL connection is closed
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
