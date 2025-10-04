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
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

// 강력한 브라우저 종료 함수
const forceKillBrowser = async (browser) => {
  try {
    console.log('🔍 모든 페이지 강제 종료 중...');
    // 모든 페이지 강제 종료
    const pages = await browser.pages();
    await Promise.all(pages.map(page => page.close().catch(() => {})));
    
    console.log('🔍 브라우저 종료 중...');
    // 브라우저 종료
    await browser.close();
    
    console.log('🔍 프로세스 완전 종료 대기 중...');
    // 프로세스 완전 종료 대기
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    console.log('🔍 남은 Chrome 프로세스 강제 종료 중...');
    // 남은 Chrome 프로세스 강제 종료
    try {
      await execAsync('pkill -f chrome || pkill -f chromium || true');
    } catch (e) {
      console.log('ℹ️ pkill 명령어 실행 중 에러 (무시):', e.message);
    }
    
    console.log('✅ 브라우저 강제 종료 완료');
  } catch (error) {
    console.log(`⚠️ 브라우저 강제 종료 중 에러: ${error.message}`);
  }
};

(async () => {
  const options = parseArguments();
  
  // 명령행 인수 파싱
  const args = process.argv.slice(2);
  const argsMap = {};
  args.forEach(arg => {
    const [key, value] = arg.split('=');
    if (key && value) {
      argsMap[key] = value;
    }
  });

  // 국가 코드 매핑
  const countryMapping = {
    'germany': 'germany',
    '독일': 'germany',
    'greece': 'greece', 
    '그리스': 'greece',
    'england': 'england',
    '영국': 'england',
    'spain': 'spain',
    '스페인': 'spain',
    'france': 'france',
    '프랑스': 'france',
    'italy': 'italy',
    '이탈리아': 'italy'
  };

  // 리그 코드 매핑
  const leagueMapping = {
    // 독일
    '2-bundesliga': '2-bundesliga',
    'bundesliga': 'bundesliga',
    '3-liga': '3-liga',
    
    // 영국
    'premier-league': 'premier-league',
    'championship': 'championship',
    'league-one': 'league-one',
    'league-two': 'league-two',
    'national-league': 'national-league',
    
    // 그리스
    'super-league': 'super-league',
    '슈퍼리그': 'super-league',
    
    // 스페인
    'laliga': 'laliga',
    'segunda-division': 'segunda-division',
    
    // 이탈리아
    'serie-a': 'serie-a',
    'serie-b': 'serie-b',
    
    // 프랑스
    'ligue-1': 'ligue-1',
    'ligue-2': 'ligue-2'
  };

  // 현재 연도 기반으로 최신 시즌 계산
  const currentYear = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1; // 1-12
  
  // 8월 이후면 현재년도-다음년도, 그 전이면 이전년도-현재년도
  let latestSeason;
  if (currentMonth >= 8) {
    latestSeason = `${currentYear}-${currentYear + 1}`;
  } else {
    latestSeason = `${currentYear - 1}-${currentYear}`;
  }

  // 명령행 인수에서 값 추출
  const countryCode = countryMapping[argsMap.country] || 'germany';
  const leagueCode = leagueMapping[argsMap.league] || '2-bundesliga';
  const fileType = argsMap.fileType || 'json';
  const seasonYear = argsMap.season || latestSeason;

  console.log(`🎯 실행 설정:`);
  console.log(`  국가: ${countryCode}`);
  console.log(`  리그: ${leagueCode}`);
  console.log(`  시즌: ${seasonYear}`);
  console.log(`  파일 형식: ${fileType}\n`);

  let browser = await puppeteer.launch({ 
    headless: options.headless !== false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-web-security',
      '--disable-features=VizDisplayCompositor',
      '--memory-pressure-off',
      '--single-process',
      '--no-zygote',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--disable-extensions',
      '--disable-plugins',
      '--disable-images',
      '--disable-javascript',
      '--max_old_space_size=256'
    ],
    protocolTimeout: 120000, // 120초 타임아웃
    timeout: 120000
  });

  // 직접 URL 구성
  const seasonUrl = `${BASE_URL}/soccer/${countryCode}/${leagueCode}-${seasonYear}/`;
  console.log(`🔗 접속 URL: ${seasonUrl}`);

  // 대화형 선택 건너뛰기
  const country = { name: countryCode, id: countryCode };
  const league = { name: leagueCode, url: `${BASE_URL}/soccer/${countryCode}/${leagueCode}/` };
  const season = { name: `${leagueCode} ${seasonYear}`, url: seasonUrl };

  // 파일명 생성
  const fileName = `soccer_${countryCode}_${leagueCode}-${seasonYear}`;
  console.log(`📁 출력 파일: ${fileName}.${fileType}\n`);

  console.info(`\n📝 Data collection has started!`);
  console.info(`The league data will be saved to: ${OUTPUT_PATH}/${fileName}.${fileType}`);

  start();
  const matchIdList = await getMatchIdList(browser, seasonUrl);
  stop();

  const progressbar = initializeProgressbar(matchIdList.length);

  const matchData = {};
  const BATCH_SIZE = 10; // 10개마다 브라우저 재시작 (20 → 10)
  const REST_TIME = 45000; // 45초 휴식 (30초 → 45초)

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
    
    // 10개마다 브라우저 재시작
    if (currentIndex > 0 && currentIndex % BATCH_SIZE === 0) {
      console.log(`\n\n⏸️  ${currentIndex}개 매치 처리 완료. 브라우저 재시작 중...`);
      
      // 강력한 브라우저 종료
      await forceKillBrowser(browser);
      
      console.log(`💤 45초 휴식 중...`);
      await new Promise(resolve => setTimeout(resolve, REST_TIME));
      
      console.log(`🔄 브라우저 재시작...\n`);
      browser = await puppeteer.launch({ 
        headless: options.headless !== false,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-web-security',
          '--disable-features=VizDisplayCompositor',
          '--memory-pressure-off',
          '--single-process',
          '--no-zygote',
          '--disable-background-timer-throttling',
          '--disable-backgrounding-occluded-windows',
          '--disable-renderer-backgrounding',
          '--disable-extensions',
          '--disable-plugins',
          '--disable-images',
          '--disable-javascript',
          '--max_old_space_size=256'
        ],
        protocolTimeout: 120000, // 120초 타임아웃
        timeout: 120000
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
      
      // 심각한 에러인 경우 강력한 브라우저 재시작
      if (error.message.includes('Target.closeTarget') || 
          error.message.includes('Navigation timeout') ||
          error.message.includes('Target.createTarget') ||
          error.message.includes('Protocol error') ||
          error.message.includes('Network.enable')) {
        
        console.log(`🚨 브라우저 상태 문제 감지. 강력한 재시작...`);
        browserRestartCount++;
        
        // 강력한 브라우저 종료
        await forceKillBrowser(browser);
        
        console.log(`💤 60초 휴식 후 브라우저 재시작...`);
        await new Promise(resolve => setTimeout(resolve, 60000));
        
        console.log(`🔄 브라우저 재시작 (${browserRestartCount}번째)...`);
        browser = await puppeteer.launch({ 
          headless: options.headless !== false,
          args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--memory-pressure-off',
            '--single-process',
            '--no-zygote',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-images',
            '--disable-javascript',
            '--max_old_space_size=256'
          ],
          protocolTimeout: 120000, // 120초 타임아웃
          timeout: 120000
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
