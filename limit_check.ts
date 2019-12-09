import {promises as fsPromises} from 'fs';
import * as path from 'path';

/* Users groups file to mock sending */
let fileHandle: fsPromises.FileHandle;
const dataFile: string = path.resolve('.', 'data_file.txt');
/* ================================= */

/* test data */
const numGroups = 2;
const numUsers = 10;
const msgPerUser = 10;
const groups = [...Array(numUsers)]
  .reduce((res, item, i) => {
    const groupId = i % numGroups;
    res[groupId] = res[groupId] || [];
    res[groupId].push(i);
    return res;
  }, {});
const groupsIds = Object.keys(groups);
/* ========= */

/* Limits/Timeouts */
const limitPeriod = 30;
const limitPerGroup = 10;
const limitPerUser = 5;
const timeoutPerGroup = limitPeriod / limitPerGroup;
const timeoutPerUser = limitPeriod / limitPerUser;
/* =============== */

/* Navigate groups and users help variables */
let groupIdx = 0; // current groupId index
let groupId = 0;  // current groupId to process
let groupUserIdxs: {[key: string]: number} = groupsIds // user indexes to process per group
  .reduce((res, gid) => {
    res[gid] = 0;
    return res;
  }, {});
let groupTimes: { [key: string]: Date } = {}; // DT of last sending per group
let userTimes: { [key: string]: Date } = {}; // DT of last sengind per user
/* ======================================== */

/* check if timeout seconds not passed since dt yet */
const waiting = (dt: Date | undefined, timeout: number): boolean => {
  if (dt) {
    const bdt = new Date();
    bdt.setSeconds(bdt.getSeconds() - timeout);
    // console.log(bdt, dt, 'Waiting')
    return bdt < dt;
  }
};
/* ================================================ */

/* Select groupId and userId to send message to */
const nextGroupAndUserId = async (): Promise<number[]> => {
  while (true) {
    let now = new Date();

    if (!groupsIds[groupIdx]) { // All groups processed. Iterate over again. And maybe we need timeout?
      groupIdx = 0;
      let minDt = Object
        .values(groupTimes)
        .sort((a, b) => a > b ? 1 : a < b ? -1 : 0)
        .shift();

      minDt = new Date(minDt.getTime());
      minDt.setSeconds(minDt.getSeconds() + timeoutPerGroup);
      if (now < minDt) {
        await delay(minDt.getTime() - now.getTime());
        now = new Date;
      }
    }
    const groupId = groupsIds[groupIdx];
    const group = groups[groupId];
    
    if (waiting(groupTimes[groupId], timeoutPerGroup)) {
      groupIdx++;
      continue;
    }

    const userId = group[groupUserIdxs[groupId]++];

    if (typeof userId == 'undefined') {
      groupUserIdxs[groupId] = 0; // next time start processing from first user in this group
      groupIdx++; // move to next group
    } else if (!waiting(userTimes[userId], timeoutPerUser)) {
      groupTimes[groupId] = now;
      userTimes[userId] = now;
      
      return [groupId, userId];
    }
  }
};
/* ============================================ */

const send = (userId: number, groupId: number, text: string): Promise<void> => {
  return fileHandle.appendFile(`${new Date().toISOString()}\t${groupId.toString()}\t${userId.toString()}\t${text}\n`);
}

const delay = (pauseMilliSeconds: number): Promise<void> => {
  return new Promise(rs => setTimeout(rs, pauseMilliSeconds));
};

/* main() */
(async (): Promise<void> => {
  fileHandle = await fsPromises.open(dataFile, 'w');

  let count = numUsers * msgPerUser;
  
  for (let i = 0; i < count; i++) {
    const [_groupId, userId] = await nextGroupAndUserId();
    console.log([_groupId, userId], 'IDS');
    await send(userId, _groupId, 'Some text');
  }
})();
