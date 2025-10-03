import puppeteer from 'puppeteer';

import { BASE_URL, OUTPUT_PATH } from './constants/index.js';

import { parseArguments } from './cli/arguments/index.js';

import { selectFileType } from './cli/prompts/fileType/index.js';
import { selectCountry } from './cli/prompts/countries/index.js';
import { selectLeague } from './cli/prompts/leagues/index.js';
import { selectSeason } from './cli/prompts/season/index.js';

import { start, stop } from './cli/loader/index.js';
import { initializeProgressbar } from './cli/progressbar/index.js';

import { getMatchIdList, getMatchData } from './scraper/services/matches/index.js';

import { handleFileType } from './files/handle/index.js';

(async () => {
  const options = parseArguments();
  let browser = await puppeteer.launch({ headless: options.headless });

  const fileType = options.fileType || (await selectFileType());
  const country = options.country ? { name: options.country } : await selectCountry(browser);
  const league = options.league ? { name: options.league } : await selectLeague(browser, country?.id);

  const season = league?.url ? await selectSeason(browser, league?.url) : { name: league?.name, url: `${BASE_URL}/football/${country?.name}/${league?.name}` };

  // URL에서 영어 경로명 추출 (한글 파일명 문제 해결)
  let fileName;
  if (season?.url) {
    const urlPath = season.url.replace(BASE_URL, '').replace('/football/', '');
    fileName = urlPath
      .toLowerCase()
      .replace(/\//g, '_')
      .replace(/[^a-z0-9_-]+/g, '_')
      .replace(/^_|_$/g, '');
  } else {
    // fallback: 기존 방식
    fileName = `${country?.name}_${season?.name}`
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_|_$/g, '');
  }

  console.info(`\n📝 Data collection has started!`);
  console.info(`The league data will be saved to: ${OUTPUT_PATH}/${fileName}.${fileType}`);

  start();
  const matchIdList = await getMatchIdList(browser, season?.url);
  stop();

  const progressbar = initializeProgressbar(matchIdList.length);

  const matchData = {};
  const BATCH_SIZE = 40; // 40개마다 브라우저 재시작
  const REST_TIME = 20000; // 20초 휴식

  for (let i = 0; i < matchIdList.length; i++) {
    const matchId = matchIdList[i];
    
    // 40개마다 브라우저 재시작
    if (i > 0 && i % BATCH_SIZE === 0) {
      console.log(`\n\n⏸️  ${i}개 매치 처리 완료. 브라우저 재시작 중...`);
      await browser.close();
      
      console.log(`💤 20초 휴식 중...`);
      await new Promise(resolve => setTimeout(resolve, REST_TIME));
      
      console.log(`🔄 브라우저 재시작...\n`);
      browser = await puppeteer.launch({ headless: options.headless });
    }
    
    try {
      matchData[matchId] = await getMatchData(browser, matchId);
      handleFileType(matchData, fileType, fileName);
    } catch (error) {
      console.error(`\n❌ 매치 ${matchId} 처리 실패: ${error.message}`);
      
      // 심각한 에러인 경우 브라우저 재시작 고려
      if (error.message.includes('Target.closeTarget') || error.message.includes('Navigation timeout')) {
        console.log(`🔄 브라우저 상태 문제 감지. 다음 배치에서 재시작 예정...`);
        
        // 현재 배치를 조기 종료하여 브라우저 재시작 트리거
        const nextBatchStart = Math.ceil((i + 1) / BATCH_SIZE) * BATCH_SIZE;
        if (i < nextBatchStart - 1) {
          console.log(`⏭️  현재 배치 건너뛰고 브라우저 재시작...`);
          i = nextBatchStart - 1; // 다음 배치 시작으로 이동
          continue;
        }
      }
      
      // 실패해도 계속 진행
    }
    
    progressbar.increment();
  }

  progressbar.stop();

  console.info('\n✅ Data collection and file writing completed!');
  console.info(`The data has been successfully saved to: ${OUTPUT_PATH}/${fileName}.${options.fileType}\n`);

  await browser.close();
})();
