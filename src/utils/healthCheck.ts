import { promises as fs } from 'fs';
import * as path from 'path';
import { ENV } from '../config/env';
import getMyBalance from './getMyBalance';
import fetchData from './fetchData';
import Logger from './logger';
import { ensureStorageDir, getStorageDir } from '../storage/fileStore';

export interface HealthCheckResult {
    healthy: boolean;
    checks: {
        database: { status: 'ok' | 'error'; message: string };
        rpc: { status: 'ok' | 'error'; message: string };
        balance: { status: 'ok' | 'error' | 'warning'; message: string; balance?: number };
        apiCredentials: { status: 'ok' | 'warning' | 'error'; message: string };
        polymarketApi: { status: 'ok' | 'error'; message: string };
    };
    timestamp: number;
}

/**
 * Perform health check on all critical components
 */
export const performHealthCheck = async (): Promise<HealthCheckResult> => {
    const checks: HealthCheckResult['checks'] = {
        database: { status: 'error', message: 'Not checked' },
        rpc: { status: 'error', message: 'Not checked' },
        balance: { status: 'error', message: 'Not checked' },
        apiCredentials: { status: 'error', message: 'Not checked' },
        polymarketApi: { status: 'error', message: 'Not checked' },
    };

    // Check file storage availability
    try {
        const dir = await ensureStorageDir();
        const probePath = path.join(dir, '.healthcheck');
        await fs.writeFile(probePath, 'ok', 'utf-8');
        await fs.unlink(probePath);
        checks.database = { status: 'ok', message: `Storage ready (${getStorageDir()})` };
    } catch (error) {
        checks.database = {
            status: 'error',
            message: `Storage check failed: ${error instanceof Error ? error.message : String(error)}`,
        };
    }

    // Check RPC endpoint
    try {
        const response = await fetch(ENV.RPC_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                jsonrpc: '2.0',
                method: 'eth_blockNumber',
                params: [],
                id: 1,
            }),
            signal: AbortSignal.timeout(5000), // 5 second timeout
        });

        if (response.ok) {
            const data = await response.json();
            if (data.result) {
                checks.rpc = { status: 'ok', message: 'RPC endpoint responding' };
            } else {
                checks.rpc = { status: 'error', message: 'Invalid RPC response' };
            }
        } else {
            checks.rpc = { status: 'error', message: `HTTP ${response.status}` };
        }
    } catch (error) {
        checks.rpc = {
            status: 'error',
            message: `RPC check failed: ${error instanceof Error ? error.message : String(error)}`,
        };
    }

    // Check USDC balance
    try {
        const balance = await getMyBalance(ENV.PROXY_WALLET);
        if (balance > 0) {
            if (balance < 10) {
                checks.balance = {
                    status: 'warning',
                    message: `Low balance: $${balance.toFixed(2)}`,
                    balance,
                };
            } else {
                checks.balance = {
                    status: 'ok',
                    message: `Balance: $${balance.toFixed(2)}`,
                    balance,
                };
            }
        } else {
            checks.balance = { status: 'error', message: 'Zero balance' };
        }
    } catch (error) {
        checks.balance = {
            status: 'error',
            message: `Balance check failed: ${error instanceof Error ? error.message : String(error)}`,
        };
    }

    // Check Polymarket API credentials (optional)
    const apiKey = ENV.POLYMARKET_API_KEY?.trim();
    const apiSecret = ENV.POLYMARKET_API_SECRET?.trim();
    const apiPassphrase = ENV.POLYMARKET_PASSPHRASE?.trim();
    const hasAnyCreds = Boolean(apiKey || apiSecret || apiPassphrase);
    const hasAllCreds = Boolean(apiKey && apiSecret && apiPassphrase);

    if (hasAllCreds) {
        checks.apiCredentials = { status: 'ok', message: 'CLOB API credentials set (env)' };
    } else if (hasAnyCreds) {
        checks.apiCredentials = {
            status: 'warning',
            message: 'Partial POLYMARKET_API_* set (will be ignored)',
        };
    } else {
        checks.apiCredentials = { status: 'ok', message: 'Using derived CLOB credentials' };
    }

    // Check Polymarket API
    try {
        const testUrl =
            'https://data-api.polymarket.com/positions?user=0x0000000000000000000000000000000000000000';
        await fetchData(testUrl);
        checks.polymarketApi = { status: 'ok', message: 'API responding' };
    } catch (error) {
        checks.polymarketApi = {
            status: 'error',
            message: `API check failed: ${error instanceof Error ? error.message : String(error)}`,
        };
    }

    // Determine overall health
    const healthy =
        checks.database.status === 'ok' &&
        checks.rpc.status === 'ok' &&
        checks.balance.status !== 'error' &&
        checks.polymarketApi.status === 'ok';

    return {
        healthy,
        checks,
        timestamp: Date.now(),
    };
};

/**
 * Log health check results
 */
export const logHealthCheck = (result: HealthCheckResult): void => {
    Logger.separator();
    Logger.header('🏥 HEALTH CHECK');
    Logger.info(`Overall Status: ${result.healthy ? '✅ Healthy' : '❌ Unhealthy'}`);
    Logger.info(
        `Storage: ${result.checks.database.status === 'ok' ? '✅' : '❌'} ${result.checks.database.message}`
    );
    Logger.info(
        `RPC: ${result.checks.rpc.status === 'ok' ? '✅' : '❌'} ${result.checks.rpc.message}`
    );
    Logger.info(
        `Balance: ${result.checks.balance.status === 'ok' ? '✅' : result.checks.balance.status === 'warning' ? '⚠️' : '❌'} ${result.checks.balance.message}`
    );
    Logger.info(
        `Polymarket API: ${result.checks.polymarketApi.status === 'ok' ? '✅' : '❌'} ${result.checks.polymarketApi.message}`
    );
    Logger.separator();
};
