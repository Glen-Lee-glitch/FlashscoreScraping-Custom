import puppeteer from 'puppeteer';

const BASE_URL = 'https://www.flashscore.com';

// 테스트용 매치 ID
const TEST_MATCH_ID = 'rToBQKk1';

async function testMatchEvents() {
  console.log('🔍 경기 이벤트 추출 테스트 시작...');
  
  const browser = await puppeteer.launch({ 
    headless: false, // 브라우저 창을 보여줌
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage'
    ]
  });

  try {
    const page = await browser.newPage();
    const matchUrl = `${BASE_URL}/match/${TEST_MATCH_ID}/#/match-summary/match-summary`;
    
    console.log(`📄 페이지 접속: ${matchUrl}`);
    await page.goto(matchUrl, { waitUntil: 'domcontentloaded' });
    
    // 페이지 로딩 대기
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    // 요소 0 (경기 이벤트) 정보 추출
    const matchEventsInfo = await page.evaluate(() => {
      // loadable complete 요소들 중 첫 번째 (요소 0)
      const loadableCompleteElements = document.querySelectorAll('.loadable.complete');
      const firstElement = loadableCompleteElements[0];
      
      if (!firstElement) {
        return { error: '요소 0을 찾을 수 없습니다' };
      }
      
      console.log('[BROWSER] 요소 0 발견, 이벤트 추출 시작...');
      
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
      const incidents = Array.from(firstElement.querySelectorAll('.smv__incident')).map((incident, index) => {
        const incidentInfo = {
          index: index,
          innerText: incident.innerText?.trim() || '',
          innerHTML: incident.innerHTML?.substring(0, 200) || '',
          className: incident.className,
          // 이벤트 분류 판단
          eventType: '기타',
          time: null,
          team: null // 홈/어웨이
        };
        
        const text = incidentInfo.innerText;
        
        // 시간 추출 (예: "38'", "46'")
        const timeMatch = text.match(/(\d+)'/);
        if (timeMatch) {
          incidentInfo.time = timeMatch[1];
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
        
        console.log(`[BROWSER] 이벤트 ${index}:`, incidentInfo);
        return incidentInfo;
      });
      
      // 전체 smv__incident 요소 개수 확인
      const allIncidents = document.querySelectorAll('.smv__incident');
      
      return {
        firstHalfScore: firstHalfScore,
        secondHalfScore: secondHalfScore,
        incidentsCount: incidents.length,
        totalIncidentsOnPage: allIncidents.length,
        incidents: incidents,
        rawText: firstElement.innerText?.substring(0, 500) || ''
      };
    });
    
    console.log('\n📊 결과:');
    console.log(`전반전 점수: ${matchEventsInfo.firstHalfScore}`);
    console.log(`후반전 점수: ${matchEventsInfo.secondHalfScore}`);
    console.log(`요소 0 내 이벤트 개수: ${matchEventsInfo.incidentsCount}`);
    console.log(`페이지 전체 이벤트 개수: ${matchEventsInfo.totalIncidentsOnPage}`);
    
    console.log('\n🎯 이벤트 상세:');
    matchEventsInfo.incidents.forEach((incident, index) => {
      console.log(`\n--- 이벤트 ${index} ---`);
      console.log(`시간: ${incident.time}`);
      console.log(`분류: ${incident.eventType}`);
      console.log(`팀: ${incident.team}`);
      console.log(`내용: ${incident.innerText}`);
      console.log(`HTML: ${incident.innerHTML}`);
    });
    
    // JSON 형식으로 정리된 결과 출력
    const organizedEvents = {
      firstHalfScore: matchEventsInfo.firstHalfScore,
      secondHalfScore: matchEventsInfo.secondHalfScore,
      events: matchEventsInfo.incidents.map(incident => ({
        type: incident.eventType,
        time: incident.time,
        team: incident.team,
        description: incident.innerText
      }))
    };
    
    console.log('\n📋 JSON 형식 정리 결과:');
    console.log(JSON.stringify(organizedEvents, null, 2));
    
    console.log('\n📝 원본 텍스트 (앞 500자):');
    console.log(matchEventsInfo.rawText);
    
    // 30초 대기 (브라우저 창을 보고 싶다면)
    console.log('\n⏰ 30초 후 브라우저가 닫힙니다...');
    await new Promise(resolve => setTimeout(resolve, 30000));
    
  } catch (error) {
    console.error('❌ 테스트 실패:', error);
  } finally {
    await browser.close();
  }
}

// 테스트 실행
testMatchEvents().catch(console.error);
