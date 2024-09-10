const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

module.exports = async function (context, myTimer) {
    context.log('KeepAlive function triggered.');

    try {
        // Use Managed Identity to connect to the database
        const credential = new DefaultAzureCredential();
        const accessToken = await credential.getToken("https://database.windows.net/");
        const sqlConfig = {
            server: process.env.SQLSERVER,
            database: process.env.SQLDATABASE,
            options: {
                encrypt: true,
                enableArithAbort: true,
                requestTimeout: 60000  // Increase SQL timeout to 60 seconds
            },
            authentication: {
                type: 'azure-active-directory-msi-app-service',
                options: {
                    token: accessToken.token
                }
            }
        };

        context.log("Connecting to the database...");
        await sql.connect(sqlConfig);

        // Run a simple query to keep the database alive
        const query = `SELECT 1`;
        const result = await new sql.Request().query(query);

        context.log("Database keep-alive query executed successfully:", result.recordset);

    } catch (err) {
        context.log("Error executing keep-alive query:", err.message);
    } finally {
        sql.close();
    }
};
