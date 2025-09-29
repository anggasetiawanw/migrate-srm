import { Client as ESClient, estypes } from "@elastic/elasticsearch";
import { Pool } from "pg";
import fs from "fs";
import cliProgress from "cli-progress";
import { Checkpoint, Device, HistoryEntity } from "./interface";

import { deviceList } from "./list_device";
// ----------------------
// 🔑 Set your migration date range here
// ----------------------
const START_DATE = new Date(Date.UTC(2025, 6, 1, 0, 0, 0)).toISOString(); // 2025-09-28T00:00:00Z
const END_DATE = new Date(Date.UTC(2025, 8, 30, 23, 59, 59)).toISOString(); // 2025-09-30T23:59:59Z

// Elasticsearch client
// ----------------------
const esClient = new ESClient({
  node: "http://103.186.0.213:9200", // adjust if needed
});

// ----------------------
// Postgres connection pool
// ----------------------
//
const pgPool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "your_db",
  password: "your_password",
  port: 5432,
});

// === Checkpoint system ===
const checkpointFile = "checkpoint.json";

function loadCheckpoint(): Checkpoint | null {
  if (fs.existsSync(checkpointFile)) {
    return JSON.parse(fs.readFileSync(checkpointFile, "utf8")) as Checkpoint;
  }
  return null;
}

function saveCheckpoint(data: Checkpoint) {
  fs.writeFileSync(checkpointFile, JSON.stringify(data, null, 2));
}

// === Count docs for each device ===
async function countDocs(
  device: Device,
  startDate: string,
  endDate: string
): Promise<number> {
  const response = await esClient.count({
    index: "history",
    query: {
      bool: {
        must: [
          { term: { code: device.code } },
          {
            range: {
              deviceTime: {
                gte: startDate,
                lte: endDate,
                format: "strict_date_optional_time",
              },
            },
          },
        ],
      },
    },
  });

  return response.count;
}

// === Migrate one device ===
async function migrateDevice(
  device: Device,
  overallBar: cliProgress.MultiBar,
  globalBar: cliProgress.SingleBar,
  batchSize = 5000,
  resumeFrom?: string
) {
  const startDate =
    resumeFrom && resumeFrom !== "DONE" ? resumeFrom : START_DATE;
  const endDate = END_DATE;

  let searchAfter: any[] | undefined = undefined;
  let totalMigrated = 0;

  // per-device progress bar
  const deviceTotal = await countDocs(device, startDate, endDate);
  const deviceBar = overallBar.create(deviceTotal, 0, { device: device.code });

  while (true) {
    const response = await esClient.search<HistoryEntity>({
      index: "history",
      size: batchSize,
      sort: [{ deviceTime: "asc" }, { createdAt: "asc" }],
      search_after: searchAfter,
      query: {
        bool: {
          must: [
            { term: { code: device.code } },
            {
              range: {
                deviceTime: {
                  gte: startDate,
                  lte: endDate,
                  format: "strict_date_optional_time",
                },
              },
            },
          ],
        },
      },
    });
    console.log(response);

    // ✅ Explicit type annotation
    const hits: estypes.SearchHit<HistoryEntity>[] = response.hits.hits;
    if (hits.length === 0) break;
    // COMMENTED OUT FOR TESTING
    console.log(`Migrating ${hits.length} records for ${device.code}...`);
    // const client = await pgPool.connect();
    // try {
    //   await client.query("BEGIN");

    //   for (const hit of hits) {
    //     const doc = hit._source as HistoryEntity;

    //     await client.query(
    //       `
    //       INSERT INTO device_status (time, "machineNumber", status)
    //       VALUES ($1, $2, $3)
    //       ON CONFLICT DO NOTHING
    //       `,
    //       [
    //         new Date(doc.deviceTime),
    //         device.code,
    //         doc.status === 1,
    //       ]
    //     );
    //   }

    //   await client.query("COMMIT");
    // } catch (err) {
    //   await client.query("ROLLBACK");
    //   console.error(`❌ Error migrating ${device.code}:`, err);
    //   throw err;
    // } finally {
    //   client.release();
    // }

    totalMigrated += hits.length;

    // checkpoint
    const lastDoc = hits[hits.length - 1]._source as HistoryEntity;
    saveCheckpoint({ deviceCode: device.code, lastTime: lastDoc.deviceTime });

    // update progress bars
    deviceBar.increment(hits.length, { device: device.code });
    globalBar.increment(hits.length); // ✅ fixed

    searchAfter = hits[hits.length - 1].sort;
  }

  deviceBar.stop();
  console.log(`✅ Finished ${device.code} (total ${totalMigrated})`);
}

// === Run migration ===
async function migrateAllDevices() {
  const checkpoint = loadCheckpoint();
  let resumeDevice: string | null = checkpoint?.deviceCode ?? null;

  // === First, get accurate total count ===
  console.log("🔍 Counting total documents in Elasticsearch...");
  let totalDocs = 0;
  for (const device of deviceList) {
    totalDocs += await countDocs(device, START_DATE, END_DATE);
  }
  console.log(`📊 Total docs to migrate: ${totalDocs}`);

  // === Progress bars ===
  const overallBar = new cliProgress.MultiBar(
    {
      format: `{device} | {bar} | {value}/{total}`,
      hideCursor: true,
      clearOnComplete: false,
    },
    cliProgress.Presets.shades_classic
  );

  // Global progress bar
  const globalBar = overallBar.create(totalDocs, 0, { device: "ALL" });

  for (const device of deviceList) {
    if (resumeDevice && device.code !== resumeDevice) {
      console.log(`⏭ Skipping ${device.code}, already migrated`);
      continue;
    }

    console.log(
      `\n🚀 Starting migration for ${device.code} (${device.machineNumber})`
    );

    await migrateDevice(
      device,
      overallBar,
      globalBar,
      5000,
      checkpoint?.lastTime
    );

    saveCheckpoint({ deviceCode: device.code, lastTime: "DONE" });

    resumeDevice = null;
  }

  overallBar.stop();
  console.log("\n🎉 All devices migrated successfully");

  if (fs.existsSync(checkpointFile)) {
    fs.unlinkSync(checkpointFile); // clean up
  }
}

// === Run main ===
migrateAllDevices().catch(console.error);
