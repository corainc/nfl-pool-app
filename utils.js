const moment = require('moment-timezone');

// Static list of NFL week end dates in UTC
const nflWeekEndDates = [
    "2024-09-09", // Week 1 ends
    "2024-09-16", // Week 2 ends
    "2024-09-23", // Week 3 ends
    "2024-09-30", // Week 4 ends
    "2024-10-07", // Week 5 ends
    "2024-10-14", // Week 6 ends
    "2024-10-21", // Week 7 ends
    "2024-10-28", // Week 8 ends
    "2024-11-04", // Week 9 ends
    "2024-11-11", // Week 10 ends
    "2024-11-18", // Week 11 ends
    "2024-11-25", // Week 12 ends
    "2024-12-02", // Week 13 ends
    "2024-12-09", // Week 14 ends
    "2024-12-16", // Week 15 ends
    "2024-12-23", // Week 16 ends
    "2024-12-30", // Week 17 ends
    "2025-01-06"  // Week 18 ends
];

function getCurrentNFLWeek() {
    // Set the timezone to U.S. Central Time
    const centralTimeOffset = 'America/Chicago';
    const currentCentralTime = moment.tz(centralTimeOffset);

    for (let i = 0; i < nflWeekEndDates.length; i++) {
        // Convert each week end date to Central Time
        const weekEndDate = moment.tz(nflWeekEndDates[i], centralTimeOffset).endOf('day');

        if (currentCentralTime.isSameOrBefore(weekEndDate)) {
            return i + 1; // Week numbers are 1-based
        }
    }

    return 18; // Default to week 18 if we're past all weeks
}

module.exports = { getCurrentNFLWeek };