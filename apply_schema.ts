/**
 * Run polymarket_schema.sql against PostgreSQL (DATABASE_URL from .env in cwd).
 *   npm run db:schema
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { config as loadEnv } from "dotenv";
import { Client } from "pg";

loadEnv({ path: resolve(process.cwd(), ".env") });

const dsn = process.env.DATABASE_URL;
if (!dsn) {
  console.error("DATABASE_URL is not set (check .env in current directory).");
  process.exit(1);
}

function toStatements(sql: string): string[] {
  const noCr = sql.replace(/\r\n/g, "\n");
  return noCr
    .split(";")
    .map((block) => {
      const lines = block.split("\n");
      return lines
        .filter((line) => {
          const t = line.replace(/\r$/, "").trim();
          if (t === "" || t.startsWith("--")) return false;
          return true;
        })
        .join("\n")
        .trim();
    })
    .filter((b) => b.length > 0);
}

async function main(): Promise<void> {
  const file = resolve(process.cwd(), "polymarket_schema.sql");
  const raw = readFileSync(file, "utf8");
  const statements = toStatements(raw);
  const client = new Client({ connectionString: dsn });
  await client.connect();
  let n = 0;
  try {
    for (const q of statements) {
      console.log(`Executing (${n + 1}/${statements.length})...`);
      await client.query(q);
      n += 1;
    }
  } finally {
    await client.end();
  }
  console.log(`OK: ${n} statement(s) applied from polymarket_schema.sql`);
}

main().catch((e: unknown) => {
  console.error(e);
  process.exit(1);
});
