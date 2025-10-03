import fs from 'fs';
import path from 'path';
import jsonexport from 'jsonexport';

export const writeCsvToFile = (data, outputPath, fileName) => {
  const filePath = path.join(outputPath, `${fileName}.csv`);

  const csvData = convertDataToCsv(data);

  jsonexport(csvData, (error, fileContent) => {
    if (error) throw error;

    try {
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, fileContent);
    } catch (error) {
      console.error(`Error creating directories or writing to CSV file:`, error);
    }
  });
};

const convertDataToCsv = (data) =>
  Object.keys(data).map((matchId) => {
    const { stage, date, status, home, away, result, statistics, odds } = data[matchId];
    const statisticsObject = {};
    const oddsObject = {};

    statistics.forEach((stat) => {
      statisticsObject[stat.category.toLowerCase().replace(/ /g, '_')] = {
        home: stat.homeValue,
        away: stat.awayValue,
      };
    });

    if (odds && odds['over-under'] && Array.isArray(odds['over-under'])) {
      // 가장 일반적인 기준점(첫 번째)의 평균 배당률만 CSV에 포함
      const mainOdds = odds['over-under'][0];
      if (mainOdds) {
        oddsObject.odds_over_under_handicap = mainOdds.handicap;
        oddsObject.odds_over_under_over = mainOdds.average.over;
        oddsObject.odds_over_under_under = mainOdds.average.under;
      }
    }

    return { matchId, stage, status, date, home, away, result, ...statisticsObject, ...oddsObject };
  });
