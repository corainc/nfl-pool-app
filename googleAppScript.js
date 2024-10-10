function importNFLData() {
  const BASE_URL = 'https://nfldatagatherer.azurewebsites.net/api/';
  const functionNames = [
    'getDraftPicks',
    'getStandings',
    'getTeamStats',
    'getUserTeamStats',
    'getUserWinTotals',
    'getWeeklyExpectedWins',
    'getWeeklyOdds',
    'getTheoreticalWins'
  ];
  functionNames.forEach(fetchDataAndWriteToSheet);
}

function fetchDataAndWriteToSheet(functionName) {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = spreadsheet.getSheetByName(functionName);

  if (!sheet) {
    sheet = spreadsheet.insertSheet(functionName);
  }

  sheet.clear();

  try {
    const url = `https://nfldatagatherer.azurewebsites.net/api/${functionName}`; 
    const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });       
    const responseCode = response.getResponseCode();

    if (responseCode !== 200) {
      throw new Error(`Failed to fetch data: HTTP ${responseCode}`);
    }

    const data = JSON.parse(response.getContentText());

    if (data && data.length > 0) {
      const headers = Object.keys(data[0]);
      sheet.appendRow(headers);

      data.forEach(record => {
        const row = headers.map(header => {
          const value = record[header];
          if (Array.isArray(value)) {
            // Extract team information for each object in the array
            return value.map(team => `${team.teamName} (${team.wins} 
wins)`).join(', ');
          }
          return value;
        });
        sheet.appendRow(row);
      });
    }
  } catch (error) {
    Logger.log(`Error fetching data for ${functionName}: ${error.message}`);     
  }
}