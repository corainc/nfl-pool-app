const sql = require('mssql');
const { DefaultAzureCredential } = require("@azure/identity");

module.exports = async function (context, req) {
    context.log('Processing request for standings...');

    try {
        // Fetch token using managed identity
        const token = await new DefaultAzureCredential().getToken("https://database.windows.net/");

        const sqlConfig = {
            server: process.env.SQLSERVER, // Accessing the environment variable
            database: process.env.SQLDATABASE, // Accessing the environment variable
            options: {
                encrypt: true, // For Azure SQL
                enableArithAbort: true
            },
            authentication: {
                type: 'azure-active-directory-msi-app-service',
                options: {
                    token: token.token // Pass the token from managed identity
                }
            }
        };

        // Establish SQL connection
        await sql.connect(sqlConfig);
        context.log("Connected to database successfully.");

        // Query to fetch data
        const result = await sql.query`SELECT * FROM Standings`;
        context.log(`Standings data retrieved: ${result.recordset.length} records found.`);

        context.res = {
            status: 200,
            body: result.recordset
        };
    } catch (err) {
        context.log(`Error fetching or updating standings: ${err.message}`);
        context.res = {
            status: 500,
            body: `Error: ${err.message}`
        };
    } finally {
        sql.close(); // Ensure the SQL connection is closed
    }
};
