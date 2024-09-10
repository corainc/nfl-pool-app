const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, req) {
    context.log('Processing request for team stats...');

    try {
        // Fetch token using managed identity
        const token = await new DefaultAzureCredential().getToken('https://database.windows.net/');

        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: {
                encrypt: true,
                enableArithAbort: true,
            },
            authentication: {
                type: 'azure-active-directory-msi-app-service',
                options: {
                    token: token.token,
                },
            },
        };

        // Establish SQL connection
        await sql.connect(sqlConfig);
        context.log('Connected to database successfully.');

        // Query to fetch team stats
        const result = await sql.query`SELECT * FROM TeamStats`;
        context.log(`Team stats data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            body: result.recordset,
        };
    } catch (err) {
        context.log(`Error fetching team stats: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error: ${err.message}`,
        };
    } finally {
        sql.close(); // Ensure the SQL connection is closed
    }
};
