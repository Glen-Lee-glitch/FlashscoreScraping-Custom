import { BASE_URL } from '../../../constants/index.js';
import { openPageAndNavigate, waitAndClick, waitForSelectorSafe } from '../../index.js';

export const getMatchIdList = async (browser, leagueSeasonUrl) => {
  const page = await openPageAndNavigate(browser, `${leagueSeasonUrl}/results`);

  // "더 보기" 버튼을 한 번만 클릭
  try {
    console.log('🔍 "더 보기" 버튼 찾는 중...');
    await waitAndClick(page, 'a[data-testid="wcl-buttonLink"] span[data-testid="wcl-scores-caption-05"]');
    console.log('✅ "더 보기" 버튼 클릭 성공');
    
    // 클릭 후 로딩 대기
    await new Promise(resolve => setTimeout(resolve, 3000));
  } catch (error) {
    console.log('ℹ️ "더 보기" 버튼이 없거나 클릭 실패:', error.message);
  }

  await waitForSelectorSafe(page, '.event__match.event__match--static.event__match--twoLine');

  const matchIdList = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('.event__match.event__match--static.event__match--twoLine')).map((element) => {
      return element?.id?.replace('g_1_', '');
    });
  });

  await page.close();
  return matchIdList;
};

// 재시도 로직을 위한 헬퍼 함수
const retryWithDelay = async (fn, maxRetries = 3, delayMs = 2000) => {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      console.log(`🔄 시도 ${attempt}/${maxRetries} 실패: ${error.message}`);
      
      if (attempt === maxRetries) {
        throw error;
      }
      
      console.log(`⏳ ${delayMs}ms 후 재시도...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
};

export const getMatchData = async (browser, matchId) => {
  return await retryWithDelay(async () => {
    const page = await openPageAndNavigate(browser, `${BASE_URL}/match/${matchId}/#/match-summary/match-summary`);

    // 동적 로딩을 위해 더 긴 대기 시간 추가
    await new Promise(resolve => setTimeout(resolve, 3000));
    await waitForSelectorSafe(page, '.duelParticipant__startTime');

    // 현재 URL에서 팀 ID 추출
    const pageUrl = page.url();
    const teamIds = extractTeamIdsFromUrl(pageUrl);

    const matchData = await extractMatchData(page, teamIds);
    
    // match_link 추가 (현재 페이지 URL)
    matchData.match_link = pageUrl;

    // 통계 페이지 로딩 재시도
    await retryWithDelay(async () => {
      await page.goto(`${BASE_URL}/match/${matchId}/#/match-summary/match-statistics/0`, { 
        waitUntil: 'domcontentloaded',
        timeout: 30000 // 20초 → 30초
      });
      await waitForSelectorSafe(page, "div[data-testid='wcl-statistics']", 15000); // 10초 → 15초
    }, 2, 2000); // 1초 → 2초 대기

    const statistics = await extractMatchStatistics(page);

    // 현재 페이지 URL에서 전체 경로 추출 (배당률용)
    const matchPathMatch = pageUrl.match(/\/match\/([^#?]+)/);
    const fullMatchPath = matchPathMatch ? matchPathMatch[1].replace(/\/$/, '') : matchId;

    // 배당률 페이지로 이동
    const odds = {};
    
    // Over/Under 배당률 수집 (재시도 로직 포함)
    try {
      const oddsUrl = `${BASE_URL}/match/${fullMatchPath}/odds/over-under/full-time/`;
      console.log(`[ODDS] Trying: ${oddsUrl}`);
      
      await retryWithDelay(async () => {
        await page.goto(oddsUrl, { waitUntil: 'networkidle2', timeout: 30000 }); // 20초 → 30초
        // 페이지 로딩 후 더 긴 대기 (동적 로딩을 위해)
        await new Promise(resolve => setTimeout(resolve, 5000)); // 3초 → 5초
      }, 2, 2000); // 1.5초 → 2초 대기
      
      const pageCheck = await page.evaluate(() => {
        return {
          hasOddsTable: document.querySelectorAll('.ui-table__row').length,
          bodyText: document.body.innerText.substring(0, 200),
          hasError: document.body.innerText.includes('에러') || document.body.innerText.includes('Error'),
        };
      });
      
      console.log(`[ODDS] Rows found: ${pageCheck.hasOddsTable}, Has error: ${pageCheck.hasError}`);
      
      if (pageCheck.hasOddsTable > 0) {
        const overUnderOdds = await extractMatchOdds(page);
        if (overUnderOdds) {
          odds['over-under'] = overUnderOdds;
          console.log(`[ODDS] Extracted ${overUnderOdds.length} over-under handicap lines`);
        }
      } else if (pageCheck.hasError) {
        console.log(`[ODDS] Error page detected: ${pageCheck.bodyText}`);
      }
    } catch (error) {
      console.log(`[ODDS] Failed for ${matchId}: ${error.message}`);
    }

    // 페이지 닫기 재시도
    try {
      await page.close();
    } catch (closeError) {
      console.log(`⚠️ 페이지 닫기 실패 (무시): ${closeError.message}`);
    }

    return { ...matchData, statistics, odds: Object.keys(odds).length > 0 ? odds : null };
  }, 2, 5000); // 최대 2회 재시도, 5초 대기 (3초 → 5초)
};

const extractTeamIdsFromUrl = (url) => {
  // URL 패턴: /match/soccer/team1-slug-TEAM1ID/team2-slug-TEAM2ID/
  // 팀 ID는 마지막 8글자 정도의 영문+숫자 조합으로 가정
  // 예: west-brom-CCBWpzjj → ID: CCBWpzjj
  const match = url.match(/\/match\/[^\/]+\/[^\/]+-([A-Za-z0-9]{6,12})\/[^\/]+-([A-Za-z0-9]{6,12})/);
  
  if (match) {
    return {
      home: match[2], // 두 번째 팀 (URL에서 뒤에 나오는 팀이 홈팀)
      away: match[1], // 첫 번째 팀 (URL에서 앞에 나오는 팀이 원정팀)
    };
  }
  
  return { home: null, away: null };
};

const extractMatchData = async (page, teamIds) => {
  const basicData = await page.evaluate(async () => {
    return {
      stage: document.querySelector('.tournamentHeader__country > a')?.innerText.trim(),
      date: document.querySelector('.duelParticipant__startTime')?.innerText.trim(),
      status: (() => {
        // 다양한 status 선택자 시도
        const statusSelectors = [
          '.fixedHeaderDuel__detailStatus',
          '.duelParticipant__status',
          '.event__time',
          '.event__stage',
          '.detailScore__status',
          '.matchInfo__status',
          '[class*="status"]',
          '[class*="time"]'
        ];
        
        for (const selector of statusSelectors) {
          const element = document.querySelector(selector);
          if (element) {
            const text = element.innerText.trim();
            if (text && text !== 'VS' && text !== 'v' && text !== '-') {
              return text;
            }
          }
        }
        return null;
      })(),
      home: {
        name: document.querySelector('.duelParticipant__home .participant__participantName.participant__overflow')?.innerText.trim(),
      },
      away: {
        name: document.querySelector('.duelParticipant__away .participant__participantName.participant__overflow')?.innerText.trim(),
      },
      result: (() => {
        // 다양한 점수 선택자 시도
        let homeScore = null;
        let awayScore = null;
        
        // 방법 1: detailScore__wrapper 사용
        const detailWrapper = document.querySelector('.detailScore__wrapper');
        if (detailWrapper) {
          const spans = detailWrapper.querySelectorAll('span:not(.detailScore__divider)');
          if (spans.length >= 2) {
            homeScore = spans[0]?.innerText.trim();
            awayScore = spans[1]?.innerText.trim();
          }
        }
        
        // 방법 2: 다른 점수 선택자들 시도
        if (!homeScore || !awayScore) {
          const scoreSelectors = [
            '.duelParticipant__score span',
            '.participant__score span',
            '.matchScore span',
            '.score span',
            '[data-testid="wcl-scores-caption-01"] span'
          ];
          
          for (const selector of scoreSelectors) {
            const scoreElements = document.querySelectorAll(selector);
            if (scoreElements.length >= 2) {
              homeScore = scoreElements[0]?.innerText.trim();
              awayScore = scoreElements[1]?.innerText.trim();
              if (homeScore && awayScore) break;
            }
          }
        }
        
        // 방법 3: 텍스트 기반 추출 (최후 수단)
        if (!homeScore || !awayScore) {
          const bodyText = document.body.innerText;
          const scoreMatch = bodyText.match(/(\d+)\s*[-:]\s*(\d+)/);
          if (scoreMatch) {
            homeScore = scoreMatch[1];
            awayScore = scoreMatch[2];
          }
        }
        
        return {
          home: homeScore || null,
          away: awayScore || null,
          regulationTime: document
            .querySelector('.detailScore__fullTime')
            ?.innerText.trim()
            .replace(/[\n()]/g, ''),
          penalties: Array.from(document.querySelectorAll('[data-testid="wcl-scores-overline-02"]'))
            .find((element) => element.innerText.trim().toLowerCase() === 'penalties')
            ?.nextElementSibling?.innerText?.trim()
            .replace(/\s+/g, ''),
        };
      })(),
    };
  });
  
  // 팀 ID 추가
  if (teamIds.home) basicData.home.id = teamIds.home;
  if (teamIds.away) basicData.away.id = teamIds.away;
  
  return basicData;
};

const extractMatchInformation = async (page) => {
  return await page.evaluate(async () => {
    const elements = Array.from(document.querySelectorAll("div[data-testid='wcl-summaryMatchInformation'] > div"));
    return elements.reduce((acc, element, index) => {
      if (index % 2 === 0) {
        acc.push({
          category: element?.textContent
            .trim()
            .replace(/\s+/g, ' ')
            .replace(/(^[:\s]+|[:\s]+$|:)/g, ''),
          value: elements[index + 1]?.innerText
            .trim()
            .replace(/\s+/g, ' ')
            .replace(/(^[:\s]+|[:\s]+$|:)/g, ''),
        });
      }
      return acc;
    }, []);
  });
};

const extractMatchStatistics = async (page) => {
  return await page.evaluate(async () => {
    return Array.from(document.querySelectorAll("div[data-testid='wcl-statistics']")).map((element) => ({
      category: element.querySelector("div[data-testid='wcl-statistics-category']")?.innerText.trim(),
      homeValue: Array.from(element.querySelectorAll("div[data-testid='wcl-statistics-value'] > strong"))?.[0]?.innerText.trim(),
      awayValue: Array.from(element.querySelectorAll("div[data-testid='wcl-statistics-value'] > strong"))?.[1]?.innerText.trim(),
    }));
  });
};

const extractMatchOdds = async (page) => {
  return await page.evaluate(async () => {
    const oddsRows = Array.from(document.querySelectorAll('.ui-table__row'));
    
    if (oddsRows.length === 0) {
      return null;
    }

    // 기준점별로 배당률을 그룹화
    const oddsDataByHandicap = {};

    oddsRows.forEach((row) => {
      try {
        // 북메이커명 추출
        const bookmakerImg = row.querySelector('.oddsCell__bookmaker img, .prematchLogo');
        const bookmakerName = bookmakerImg?.getAttribute('title') || bookmakerImg?.getAttribute('alt') || 'Unknown';

        // 기준점 추출
        const handicapElement = row.querySelector('span[data-testid="wcl-oddsValue"]');
        const handicap = handicapElement?.innerText.trim() || null;

        if (!handicap) return;

        // Over/Under 배당률 추출
        const oddsCells = Array.from(row.querySelectorAll('a.oddsCell__odd'));
        
        // span 태그에서 텍스트 추출
        let overOdds = null;
        let underOdds = null;
        
        if (oddsCells.length >= 2) {
          const overSpan = oddsCells[0]?.querySelector('span:not(.arrow):not(.externalLink-ico)');
          const underSpan = oddsCells[1]?.querySelector('span:not(.arrow):not(.externalLink-ico)');
          
          overOdds = overSpan?.innerText.trim() || oddsCells[0]?.innerText.trim().split('\n')[0] || null;
          underOdds = underSpan?.innerText.trim() || oddsCells[1]?.innerText.trim().split('\n')[0] || null;
        }

        if (!overOdds && !underOdds) return;

        // 기준점별로 그룹화
        if (!oddsDataByHandicap[handicap]) {
          oddsDataByHandicap[handicap] = [];
        }

        oddsDataByHandicap[handicap].push({
          bookmaker: bookmakerName,
          over: overOdds,
          under: underOdds,
        });
      } catch (error) {
        // 개별 row 파싱 실패는 무시
      }
    });

    // 각 기준점별로 평균 계산
    const oddsData = Object.keys(oddsDataByHandicap).map((handicap) => {
      const bookmakerOdds = oddsDataByHandicap[handicap];
      
      // 평균 계산
      const validOverOdds = bookmakerOdds.map(b => parseFloat(b.over)).filter(v => !isNaN(v));
      const validUnderOdds = bookmakerOdds.map(b => parseFloat(b.under)).filter(v => !isNaN(v));
      
      const averageOver = validOverOdds.length > 0 
        ? (validOverOdds.reduce((a, b) => a + b, 0) / validOverOdds.length).toFixed(2)
        : null;
      const averageUnder = validUnderOdds.length > 0
        ? (validUnderOdds.reduce((a, b) => a + b, 0) / validUnderOdds.length).toFixed(2)
        : null;

      return {
        handicap: handicap,
        average: {
          over: averageOver,
          under: averageUnder,
        },
        bookmakers: bookmakerOdds,
      };
    });

    return oddsData.length > 0 ? oddsData : null;
  });
};
