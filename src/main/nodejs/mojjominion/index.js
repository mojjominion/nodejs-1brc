import * as fs from "fs";
import * as fsp from "fs/promises";
import * as os from 'os'
import { parentPort, workerData, Worker, isMainThread } from "worker_threads";

// Copied some snippets from the solution below
// CREDITS: https://github.com/1brc/nodejs/blob/main/src/main/nodejs/Edgar-P-yan/index.js

const filePath = process.argv[2];
const WORKERS = os.cpus().length;
const SEMI_COLON = ";".charCodeAt(0);
const NEW_LINE = "\n".charCodeAt(0);


/**
 * @param {Function} fnc 
 */
// async function perf(fnc) {
//   console.time(fnc.name.toString());
//   await fnc();
//   console.timeEnd(fnc.name.toString());
// }

if (isMainThread) {
  runMainThread();
} else {
  const { filePath: filename, start, end } = workerData;
  readFileSection(+start, +end);
  const map = new Map();
  /**
   * @param {number} start 
   * @param {number} end 
   */
  function readFileSection(start, end) {
    /** @type Buffer */
    let leftoverChunk = undefined;
    const readStream = fs.createReadStream(filename, { start, end });
    readStream.on(
      "data",
      (
        /** @type  Buffer */
        buffer
      ) => {
        let chunk = buffer;
        if (leftoverChunk) {
          chunk = Buffer.allocUnsafe(leftoverChunk.length + buffer.length);
          leftoverChunk.copy(chunk, 0);
          buffer.copy(chunk, leftoverChunk.length);
          leftoverChunk = null;
        }
        let index = chunk.lastIndexOf(NEW_LINE);
        if (index < chunk.length - 1) {
          leftoverChunk = chunk.subarray(index + 1);
        }
        parseChunkLines(chunk.subarray(0, index), map);
      });
    readStream.on("end", () => {
      parentPort.postMessage(map);
    });
  }
}

/**
 * @typedef {Object} MapEntry
 * @property {number} sum - The sum of all temperatures.
 * @property {number} count - The count of entries
 * @property {number} min
 * @property {number} max
 */
async function runMainThread() {
  let promises = [];
  const fileSize = (await fsp.stat(filePath)).size;
  const fileChuckSize = Math.ceil(fileSize / WORKERS);

  async function getLinedChunks() {
    const MAX_LINE_LENGTH = 100 + 1 + 4 + 1;
    let offset = 0;
    let buf = Buffer.alloc(MAX_LINE_LENGTH);
    const chunkOffsets = [0];
    const file = await fsp.open(filePath);
    while (true) {
      offset += fileChuckSize;
      if (offset >= fileSize) {
        chunkOffsets.push(fileSize);
        break;
      }
      await file.read(buf, 0, MAX_LINE_LENGTH, offset);
      let index = buf.indexOf(NEW_LINE);
      buf.fill(0);
      if (index == -1) {
        chunkOffsets.push(fileSize);
        break;
      }
      offset += index + 1;
      chunkOffsets.push(offset);
    }
    await file.close();
    return chunkOffsets;
  }

  const chunks = await getLinedChunks();
  /** @type Map<string, MapEntry> */
  const finalResult = new Map();
  for (let i = 0; i < chunks.length - 1; i++) {
    const workerThread = new Worker(
      new URL(import.meta.resolve('./index.js')),
      {
        workerData: { filePath, start: chunks[i], end: chunks[i + 1] },
      });
    const promise = new Promise((resolve) => {
      workerThread.on(
        "message",
        (
          /**
           * @type Map<string, MapEntry>
           */
          message
        ) => {
          // console.log(
          //   `Worker ${i + 1} finished reading ${message.size} items.`
          // );
          for (let [key, value] of message.entries()) {
            const existing = finalResult.get(key);
            if (existing) {
              existing.min = Math.min(existing.min, value.min);
              existing.max = Math.max(existing.max, value.max);
              existing.sum += value.sum;
              existing.count += value.count;
            } else {
              finalResult.set(key, value);
            }
          }
          resolve();
        }
      );
    });
    promises.push(promise);
  }
  await Promise.all(promises);
  printDB(finalResult);
  // divertToFile(() => printDB(finalResult));
}

/**
 * @param {Buffer} buff : '-22.3', '22.0', '2.2', '-0.2',
 */
function parseTemperature(buff) {
  let temperature = 0;
  let sign = buff[0] === "-".charCodeAt(0) ? -1 : 1;
  for (let i = 0; i < buff.length; i++) {
    if (buff[i] == ".".charCodeAt(0)) continue;
    if (buff[i] == "-".charCodeAt(0)) continue;
    temperature *= 10;
    temperature += buff[i] - "0".charCodeAt(0);
  }
  return sign * temperature;
}

// Function to extract lines from buffer chunk
/**
 * @param {Buffer} chunk
 * @param {Map} map
 */
function parseChunkLines(chunk, map) {
  let start = 0;
  for (let i = 0; i < chunk.length + 1; i++) {
    if (chunk[i] !== NEW_LINE && i <= chunk.length - 1) continue;
    let colonIndex = chunk.indexOf(SEMI_COLON, start);
    let nameBuff = chunk.subarray(start, colonIndex);
    let tempBuff = chunk.subarray(colonIndex + 1, i);
    start = i + 1;

    let name = nameBuff.toString();
    let temperature = parseTemperature(tempBuff)
    let existing = map.get(name);
    if (existing) {
      existing.count += 1;
      existing.sum += temperature;
      existing.min = Math.min(existing.min, temperature);
      existing.max = Math.max(existing.max, temperature);
    } else {
      map.set(name, { count: 1, sum: temperature, min: temperature, max: temperature });
    }
  }
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 * @returns {string}
 */
function round(num) {
  const fixed = Math.round(10 * num) / 10;
  return fixed.toFixed(1);
}
/**
 * @param {Function} fnc 
 */
// function divertToFile(fnc) {
//   let stream = fs.createWriteStream("indexout.txt");
//   let stdout = process.stdout.write;
//   let stderr = process.stderr.write;
//   process.stdout.write = process.stderr.write = stream.write.bind(stream);
//   fnc();
//   process.stdout.write = stdout;
//   process.stderr.write = stderr;
//   stream.end();
// }

/**
 * @param {Map<string, MapEntry>} db 
 */
export function printDB(db) {
  const sortedStations = Array.from(db.keys()).sort();
  process.stdout.write("{");
  for (let i = 0; i < sortedStations.length; i++) {
    if (i > 0) {
      process.stdout.write(", ");
    }
    const data = db.get(sortedStations[i]);
    process.stdout.write(sortedStations[i]);
    process.stdout.write("=");
    process.stdout.write(
      round(data.min / 10) +
      "/" +
      round(data.sum / 10 / data.count) +
      "/" +
      round(data.max / 10)
    );
  }
  process.stdout.write("}\n");
}

