/** Optional TEST acceptance helper. Install mssql separately; never contains credentials. */
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const require = createRequire(import.meta.url);
export const sql = require(process.env.CDC_MSSQL_MODULE || 'mssql');

export function testConfig() {
  const file = process.env.CDC_TEST_CONFIG;
  if (!file) throw new Error('Set CDC_TEST_CONFIG to a private JSON file (see deployment evidence).');
  const config = JSON.parse(readFileSync(file, 'utf8'));
  if (config.server !== '192.168.99.133' || config.database !== 'tdev_technophar' || config.tenantId !== 'TECHNO') {
    throw new Error('This acceptance script only permits TECHNO / .133 / tdev_technophar TEST.');
  }
  return config;
}

export async function connectTest(config = testConfig()) {
  return new sql.ConnectionPool({
    server: config.server, database: config.database,
    user: config.username, password: config.password,
    options: { encrypt: false, trustServerCertificate: true, useUTC: true },
    connectionTimeout: 10000, requestTimeout: 30000,
  }).connect();
}
