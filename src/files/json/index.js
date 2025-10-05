import fs from 'fs';
import path from 'path';

export const writeJsonToFile = (data, outputPath, fileName) => {
  const filePath = path.join(outputPath, `${fileName}.json`);
  
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    
    // 기존 파일이 있으면 읽어서 병합, 없으면 새로 생성
    let existingData = {};
    if (fs.existsSync(filePath)) {
      try {
        const existingContent = fs.readFileSync(filePath, 'utf8');
        existingData = JSON.parse(existingContent);
      } catch (parseError) {
        console.log(`⚠️ 기존 JSON 파일 파싱 실패, 새로 시작: ${parseError.message}`);
        existingData = {};
      }
    }
    
    // 데이터 병합 (새 데이터가 우선)
    const mergedData = { ...existingData, ...data };
    const fileContent = JSON.stringify(mergedData, null, 2);
    
    fs.writeFileSync(filePath, fileContent);
  } catch (error) {
    console.error(`Error creating directories or writing to JSON file:`, error);
  }
};
