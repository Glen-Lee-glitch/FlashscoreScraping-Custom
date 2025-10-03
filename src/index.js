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
  const BATCH_SIZE = 20; // 20개마다 브라우저 재시작 (40 → 20)
  const REST_TIME = 30000; // 30초 휴식 (20초 → 30초)

  let currentIndex = 0;
  let browserRestartCount = 0;
  
  // 중단점 파일 경로
  const checkpointFile = `${OUTPUT_PATH}/${fileName}_checkpoint.json`;
  
  // 기존 중단점이 있는지 확인
  try {
    const fs = await import('fs');
    if (fs.existsSync(checkpointFile)) {
      const checkpointData = JSON.parse(fs.readFileSync(checkpointFile, 'utf8'));
      currentIndex = checkpointData.lastProcessedIndex || 0;
      browserRestartCount = checkpointData.browserRestartCount || 0;
      
      console.log(`\n📍 중단점 발견! 인덱스 ${currentIndex}부터 재개합니다.`);
      console.log(`🔄 이전 브라우저 재시작 횟수: ${browserRestartCount}`);
    }
  } catch (error) {
    console.log(`ℹ️ 중단점 파일 읽기 실패, 처음부터 시작: ${error.message}`);
  }

  while (currentIndex < matchIdList.length) {
    const matchId = matchIdList[currentIndex];
    
    // 20개마다 브라우저 재시작
    if (currentIndex > 0 && currentIndex % BATCH_SIZE === 0) {
      console.log(`\n\n⏸️  ${currentIndex}개 매치 처리 완료. 브라우저 재시작 중...`);
      
      // 강제 종료
      try {
        await browser.close();
      } catch (closeError) {
        console.log(`⚠️ 브라우저 닫기 실패, 강제 종료: ${closeError.message}`);
      }
      
      console.log(`💤 30초 휴식 중...`);
      await new Promise(resolve => setTimeout(resolve, REST_TIME));
      
      console.log(`🔄 브라우저 재시작...\n`);
      browser = await puppeteer.launch({ 
        headless: options.headless,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-web-security',
          '--disable-features=VizDisplayCompositor',
          '--memory-pressure-off'
        ],
        protocolTimeout: 60000, // 60초 타임아웃
        timeout: 60000
      });
      browserRestartCount++;
    }
    
    try {
      matchData[matchId] = await getMatchData(browser, matchId);
      handleFileType(matchData, fileType, fileName);
      
      // 성공적으로 처리된 경우에만 인덱스 증가
      currentIndex++;
      progressbar.increment();
      
      // 중단점 저장 (10개마다)
      if (currentIndex % 10 === 0) {
        try {
          const fs = await import('fs');
          const checkpointData = {
            lastProcessedIndex: currentIndex,
            browserRestartCount: browserRestartCount,
            timestamp: new Date().toISOString(),
            totalMatches: matchIdList.length
          };
          fs.writeFileSync(checkpointFile, JSON.stringify(checkpointData, null, 2));
        } catch (error) {
          console.log(`⚠️ 중단점 저장 실패: ${error.message}`);
        }
      }
      
    } catch (error) {
      console.error(`\n❌ 매치 ${matchId} 처리 실패: ${error.message}`);
      
      // 심각한 에러인 경우 즉시 브라우저 재시작
      if (error.message.includes('Target.closeTarget') || 
          error.message.includes('Navigation timeout') ||
          error.message.includes('Target.createTarget') ||
          error.message.includes('Protocol error')) {
        
        console.log(`🚨 브라우저 상태 문제 감지. 즉시 재시작...`);
        browserRestartCount++;
        
        try {
          await browser.close();
        } catch (closeError) {
          console.log(`⚠️ 브라우저 닫기 실패 (무시): ${closeError.message}`);
        }
        
        console.log(`💤 30초 휴식 후 브라우저 재시작...`);
        await new Promise(resolve => setTimeout(resolve, 30000));
        
        console.log(`🔄 브라우저 재시작 (${browserRestartCount}번째)...`);
        browser = await puppeteer.launch({ 
          headless: options.headless,
          args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--memory-pressure-off'
          ],
          protocolTimeout: 60000, // 60초 타임아웃
          timeout: 60000
        });
        
        console.log(`📍 인덱스 ${currentIndex}부터 재개...`);
        // 인덱스는 증가시키지 않고 같은 매치를 다시 시도
        continue;
      }
      
      // 일반적인 에러는 건너뛰고 다음 매치로
      console.log(`⏭️  매치 ${matchId} 건너뛰고 계속 진행...`);
      currentIndex++;
      progressbar.increment();
    }
  }

  progressbar.stop();

  // 완료 후 중단점 파일 삭제
  try {
    const fs = await import('fs');
    if (fs.existsSync(checkpointFile)) {
      fs.unlinkSync(checkpointFile);
      console.log(`🗑️ 중단점 파일 삭제 완료`);
    }
  } catch (error) {
    console.log(`⚠️ 중단점 파일 삭제 실패: ${error.message}`);
  }

  console.info('\n✅ Data collection and file writing completed!');
  console.info(`The data has been successfully saved to: ${OUTPUT_PATH}/${fileName}.${options.fileType}`);
  console.info(`🔄 총 브라우저 재시작 횟수: ${browserRestartCount}\n`);

  await browser.close();
})();
