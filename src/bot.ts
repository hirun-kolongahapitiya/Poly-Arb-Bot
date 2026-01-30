import connectDB, { closeDB } from './config/db';
import { ENV } from './config/env';
import createClobClient from './utils/createClobClient';
import tradeExecutor, { stopTradeExecutor } from './services/tradeExecutor';
import tradeMonitor, { stopTradeMonitor } from './services/tradeMonitor';
import Logger from './utils/logger';
import { performHealthCheck, logHealthCheck } from './utils/healthCheck';
import type { ClobClient } from '@polymarket/clob-client';

type BotState = 'stopped' | 'starting' | 'running' | 'stopping' | 'error';

const USER_ADDRESSES = ENV.USER_ADDRESSES;
const PROXY_WALLET = ENV.PROXY_WALLET;

let state: BotState = 'stopped';
let startedAt: number | null = null;
let lastError: string | null = null;
let clobClient: ClobClient | null = null;

const ensureDbConnection = async () => {
    await connectDB();
};

const logWelcome = () => {
    const colors = {
        reset: '\x1b[0m',
        yellow: '\x1b[33m',
        cyan: '\x1b[36m',
    };

    console.log(`\n${colors.yellow}dY'­ First time running the bot?${colors.reset}`);
    console.log(`   Read the guide: ${colors.cyan}GETTING_STARTED.md${colors.reset}`);
    console.log(`   Run health check: ${colors.cyan}npm run health-check${colors.reset}\n`);
};

export const getBotStatus = () => {
    const uptimeSeconds = startedAt ? Math.floor((Date.now() - startedAt) / 1000) : 0;
    return {
        state,
        startedAt,
        uptimeSeconds,
        lastError,
        traders: USER_ADDRESSES.length,
        proxyWallet: PROXY_WALLET,
    };
};

export const startBot = async (options?: { showWelcome?: boolean; runHealthCheck?: boolean }) => {
    if (state === 'running' || state === 'starting') {
        return getBotStatus();
    }

    state = 'starting';
    lastError = null;

    try {
        if (options?.showWelcome) {
            logWelcome();
        }

        await ensureDbConnection();
        Logger.startup(USER_ADDRESSES, PROXY_WALLET);

        if (options?.runHealthCheck ?? true) {
            Logger.info('Performing initial health check...');
            const healthResult = await performHealthCheck();
            logHealthCheck(healthResult);

            if (!healthResult.healthy) {
                Logger.warning('Health check failed, but continuing startup...');
            }
        }

        Logger.info('Initializing CLOB client...');
        clobClient = await createClobClient();
        Logger.success('CLOB client ready');

        Logger.separator();
        Logger.info('Starting trade monitor...');
        tradeMonitor();

        Logger.info('Starting trade executor...');
        tradeExecutor(clobClient);

        startedAt = Date.now();
        state = 'running';
    } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
        state = 'error';
        throw error;
    }

    return getBotStatus();
};

export const stopBot = async (options?: { closeDb?: boolean }) => {
    if (state === 'stopped' || state === 'stopping') {
        return getBotStatus();
    }

    state = 'stopping';

    try {
        stopTradeMonitor();
        stopTradeExecutor();

        Logger.info('Waiting for services to finish current operations...');
        await new Promise((resolve) => setTimeout(resolve, 2000));

        if (options?.closeDb ?? true) {
            await closeDB();
        }

        state = 'stopped';
        startedAt = null;
    } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
        state = 'error';
        throw error;
    }

    return getBotStatus();
};

export const getBotClient = () => clobClient;