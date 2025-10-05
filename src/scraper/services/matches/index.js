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
    await new Promise(resolve => setTimeout(resolve, 2000));
    await waitForSelectorSafe(page, '.duelParticipant__startTime');
    
    // 이벤트 데이터 로딩을 위한 추가 대기
    await new Promise(resolve => setTimeout(resolve, 2000));
    await waitForSelectorSafe(page, '.loadable.complete', 5000);

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

    // mid 파라미터 추출
    const midMatch = pageUrl.match(/\?mid=([^#&]+)/);
    const midParam = midMatch ? midMatch[1] : matchId;

    // 배당률 페이지로 이동
    const odds = {};
    
    // Over/Under 배당률 수집 (재시도 로직 포함)
    try {
      const oddsUrl = `${BASE_URL}/match/${fullMatchPath}/odds/over-under/full-time/?mid=${midParam}`;
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
    // 경기 이벤트 추출 (요소 0)
    let matchEvents = null;
    try {
      const loadableCompleteElements = document.querySelectorAll('.loadable.complete');
      const firstElement = loadableCompleteElements[0];
      
      if (firstElement) {
        // 전반전/후반전 점수 추출
        let firstHalfScore = null;
        let secondHalfScore = null;
        
        const fullText = firstElement.innerText;
        
        // "1ST HALF" 다음에 나오는 점수 찾기
        const firstHalfMatch = fullText.match(/1ST HALF\s*(\d+\s*-\s*\d+)/i);
        if (firstHalfMatch) {
          firstHalfScore = firstHalfMatch[1];
        }
        
        // "2ND HALF" 다음에 나오는 점수 찾기
        const secondHalfMatch = fullText.match(/2ND HALF\s*(\d+\s*-\s*\d+)/i);
        if (secondHalfMatch) {
          secondHalfScore = secondHalfMatch[1];
        }
        
        // 백업: 단순 점수 패턴 찾기
        if (!firstHalfScore || !secondHalfScore) {
          const scoreMatches = fullText.match(/(\d+)\s*-\s*(\d+)/g);
          if (scoreMatches && scoreMatches.length >= 2) {
            if (!firstHalfScore) firstHalfScore = scoreMatches[0];
            if (!secondHalfScore) secondHalfScore = scoreMatches[1];
          }
        }
        
        // smv__incident 요소들 추출
        const incidents = Array.from(firstElement.querySelectorAll('.smv__incident')).map((incident) => {
          const incidentInfo = {
            eventType: '기타',
            time: null,
            team: null,
            description: incident.innerText?.trim() || ''
          };
          
          const text = incidentInfo.description;
          
          // 시간 추출 (예: "38'", "46'", "90+4'")
          const timeMatch = text.match(/(\d+)'/);
          if (timeMatch) {
            incidentInfo.time = parseInt(timeMatch[1]);
          }
          
          // 이벤트 분류 (HTML 구조 기반)
          const iconElement = incident.querySelector('.smv__incidentIcon, .smv__incidentIconSub');
          if (iconElement) {
            const iconHTML = iconElement.innerHTML;
            if (iconHTML.includes('wcl-icon-soccer') || iconHTML.includes('Goal')) {
              incidentInfo.eventType = '골';
            } else if (iconHTML.includes('substitution')) {
              incidentInfo.eventType = '교체';
            } else if (iconHTML.includes('card-ico') || iconHTML.includes('Yellow Card') || iconHTML.includes('Red Card')) {
              incidentInfo.eventType = '카드';
            }
          }
          
          // 텍스트 기반 백업 분류
          if (incidentInfo.eventType === '기타') {
            if (text.includes('골') || text.includes('Goal')) {
              incidentInfo.eventType = '골';
            } else if (text.includes('교체') || text.includes('Substitution')) {
              incidentInfo.eventType = '교체';
            } else if (text.includes('카드') || text.includes('Card')) {
              incidentInfo.eventType = '카드';
            }
          }
          
          // 팀 판단 (홈/어웨이) - HTML 구조 분석
          let teamInfo = null;
          
          // 부모 요소 체인을 따라가며 홈/어웨이 정보 찾기
          let currentElement = incident;
          while (currentElement && !teamInfo) {
            const className = currentElement.className;
            const classList = Array.from(currentElement.classList);
            const dataTestId = currentElement.getAttribute('data-testid');
            
            // 홈팀 관련 클래스나 속성 찾기
            if (className.includes('home') || 
                classList.some(cls => cls.includes('home')) ||
                dataTestId?.includes('home')) {
              teamInfo = '홈';
              break;
            }
            
            // 어웨이팀 관련 클래스나 속성 찾기
            if (className.includes('away') || 
                classList.some(cls => cls.includes('away')) ||
                dataTestId?.includes('away')) {
              teamInfo = '어웨이';
              break;
            }
            
            currentElement = currentElement.parentElement;
          }
          
          // 여전히 찾지 못했다면 위치 기반 추정
          if (!teamInfo) {
            // 요소의 상대적 위치로 판단
            const rect = incident.getBoundingClientRect();
            const containerRect = firstElement.getBoundingClientRect();
            
            // 컨테이너의 중앙점 기준으로 왼쪽은 홈팀, 오른쪽은 어웨이팀
            const centerX = containerRect.left + containerRect.width / 2;
            
            if (rect.left < centerX) {
              teamInfo = '홈';
            } else {
              teamInfo = '어웨이';
            }
          }
          
          incidentInfo.team = teamInfo;
          
          // 골의 경우 선수명과 어시스트 추출
          if (incidentInfo.eventType === '골') {
            const lines = text.split('\n').filter(line => line.trim());
            if (lines.length >= 2) {
              incidentInfo.player = lines[1].trim(); // 골득점자
              if (text.includes('(') && text.includes(')')) {
                const assistMatch = text.match(/\(([^)]+)\)/);
                if (assistMatch) {
                  incidentInfo.assist = assistMatch[1].trim();
                }
              }
            }
          }
          
          // 교체의 경우 선수명 추출
          if (incidentInfo.eventType === '교체') {
            const lines = text.split('\n').filter(line => line.trim());
            if (lines.length >= 3) {
              incidentInfo.player_out = lines[1].trim(); // 나간 선수
              incidentInfo.player_in = lines[2].trim();  // 들어온 선수
            }
          }
          
          // 카드의 경우 선수명과 카드 타입 추출
          if (incidentInfo.eventType === '카드') {
            const lines = text.split('\n').filter(line => line.trim());
            if (lines.length >= 2) {
              incidentInfo.player = lines[1].trim(); // 카드 받은 선수
            }
            
            // 카드 타입 추출
            if (iconElement && iconElement.innerHTML.includes('yellowCard')) {
              incidentInfo.card_type = 'Yellow Card';
            } else if (iconElement && iconElement.innerHTML.includes('redCard')) {
              incidentInfo.card_type = 'Red Card';
            } else if (text.includes('Yellow') || text.includes('노란')) {
              incidentInfo.card_type = 'Yellow Card';
            } else if (text.includes('Red') || text.includes('빨간')) {
              incidentInfo.card_type = 'Red Card';
            }
          }
          
          return incidentInfo;
        });
        
        matchEvents = {
          firstHalfScore: firstHalfScore,
          secondHalfScore: secondHalfScore,
          events: incidents
        };
      }
    } catch (error) {
      console.log('[BROWSER] 경기 이벤트 추출 실패:', error.message);
    }
    
    // 디버깅: 요소 0 존재 여부 확인
    const loadableCompleteElements = document.querySelectorAll('.loadable.complete');
    console.log(`[BROWSER] loadable complete 요소 개수: ${loadableCompleteElements.length}`);
    if (loadableCompleteElements.length > 0) {
      const firstElement = loadableCompleteElements[0];
      console.log(`[BROWSER] 첫 번째 요소 텍스트: ${firstElement.innerText.substring(0, 200)}`);
      const smvIncidents = firstElement.querySelectorAll('.smv__incident');
      console.log(`[BROWSER] smv__incident 요소 개수: ${smvIncidents.length}`);
    }

    return {
      stage: document.querySelector('.tournamentHeader__country > a')?.innerText.trim(),
      date: document.querySelector('.duelParticipant__startTime')?.innerText.trim(),
      events: matchEvents, // 경기 이벤트 정보 추가
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
