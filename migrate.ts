import { Client as ESClient, estypes } from "@elastic/elasticsearch";
import { Pool } from "pg";
import fs from "fs";
import cliProgress from "cli-progress";
import { Checkpoint, Device, HistoryEntity } from "./interface";
import { deviceList } from "./list_device";

// === Config ===
const ES_INDEX = "history";
const RANGE_START = "2024-05-01T00:00:00Z"; // migrate from May 2024
const RANGE_END = new Date().toISOString();
const BATCH_SIZE = 5000;
const checkpointFile = "checkpoint.json";

// === Clients ===
const esClient = new ESClient({ node: "http://103.186.0.213:9200" });

const pgPool = new Pool({
  user: "srm",
  host: "199.241.138.82",
  database: "srmdb",
  password: "54cR9rVxcl0EZgrp",
  port: 5433,
});

// === Checkpoint helpers ===
function loadCheckpoint(): Checkpoint | null {
  if (fs.existsSync(checkpointFile)) {
    return JSON.parse(fs.readFileSync(checkpointFile, "utf8")) as Checkpoint;
  }
  return null;
}
function saveCheckpoint(data: Checkpoint) {
  fs.writeFileSync(checkpointFile, JSON.stringify(data, null, 2));
}

// ----------------------
// Helpers
// ----------------------
async function countDocs(
  device: Device,
  startDate: string,
  endDate: string
): Promise<number> {
  const response = await esClient.count({
    index: ES_INDEX,
    query: {
      bool: {
        must: [{ match: { code: device.code } }],
        filter: {
          range: { deviceTime: { gte: startDate, lte: endDate } },
        },
      },
    },
  });
  return response.count ?? 0;
}

async function searchBatch(
  device: Device,
  batchSize: number,
  searchAfter?: any[],
  startDate = RANGE_START,
  endDate = RANGE_END
): Promise<estypes.SearchHit<HistoryEntity>[]> {
  const searchParams: estypes.SearchRequest = {
    index: ES_INDEX,
    size: batchSize,
    sort: [{ deviceTime: "asc" }],
    query: {
      bool: {
        must: [{ match: { code: device.code } }],
        filter: {
          range: { deviceTime: { gte: startDate, lte: endDate } },
        },
      },
    },
  };

  if (searchAfter) {
    (searchParams as any).search_after = searchAfter;
  }

  const response = await esClient.search<HistoryEntity>(searchParams);
  return response.hits.hits;
}

async function insertBatch(
  rows: { time: Date; machineNumber: string; status: boolean }[]
) {
  if (rows.length === 0) return;

  const values = rows
    .map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
    .join(",");

  const params = rows.flatMap((r) => [r.time, r.machineNumber, r.status]);

  await pgPool.query(
    `
    INSERT INTO device_status (time, "machineNumber", status)
    VALUES ${values}
    ON CONFLICT DO NOTHING
    `,
    params
  );
}

// ----------------------
// Migration logic
// ----------------------
async function migrateDevice(
  device: Device,
  multiBar: cliProgress.MultiBar,
  totalCounter: cliProgress.SingleBar,
  deviceTotal: number
) {
  console.log(
    `\n🚀 Starting migration for ${device.machineNumber} (${device.code})`
  );

  const deviceBar = multiBar.create(deviceTotal, 0, {
    name: device.machineNumber,
  });

  let migrated = 0;
  let searchAfter: any[] | undefined;

  while (true) {
    const hits = await searchBatch(device, BATCH_SIZE, searchAfter);
    if (hits.length === 0) break;

    const rows = hits.map((hit) => ({
      time: new Date(hit._source!.deviceTime),
      machineNumber: device.machineNumber,
      status: hit._source!.status === 1,
    }));

    await insertBatch(rows);

    migrated += rows.length;
    deviceBar.update(migrated);
    totalCounter.increment(rows.length);

    searchAfter = hits[hits.length - 1].sort;
  }

  deviceBar.update(deviceTotal);
  console.log(
    `✅ Finished ${device.machineNumber} (migrated ${migrated} docs)`
  );
}

async function main() {
  console.log("🔍 Counting total documents in Elasticsearch...");

  let grandTotal = 0;
  const deviceTotals: Record<string, number> = {};
  for (const device of deviceList) {
    const count = await countDocs(device, RANGE_START, RANGE_END);
    deviceTotals[device.code] = count;
    grandTotal += count;
  }

  console.log(`📊 Total docs to migrate: ${grandTotal}`);

  const multiBar = new cliProgress.MultiBar(
    {
      clearOnComplete: false,
      hideCursor: true,
      format: "{name} | {bar} | {value}/{total}",
    },
    cliProgress.Presets.shades_classic
  );

  const totalCounter = multiBar.create(grandTotal, 0, { name: "ALL" });

  // 🚀 Sequential migration (one device at a time)
  for (const device of deviceList) {
    await migrateDevice(
      device,
      multiBar,
      totalCounter,
      deviceTotals[device.code]
    );
  }

  multiBar.stop();
  console.log("\n🎉 All devices migrated successfully");
}

main()
  .catch((err) => {
    console.error("❌ Migration failed", err);
    process.exit(1);
  })
  .finally(async () => {
    await pgPool.end();
  });
