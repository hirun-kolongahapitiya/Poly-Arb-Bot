import { ethers } from 'ethers';
import { ClobClient } from '@polymarket/clob-client';
import { SignatureType } from '@polymarket/order-utils';
import { ENV } from '../config/env';
import Logger from './logger';

const PROXY_WALLET = ENV.PROXY_WALLET;
const PRIVATE_KEY = ENV.PRIVATE_KEY;
const CLOB_HTTP_URL = ENV.CLOB_HTTP_URL;
const RPC_URL = ENV.RPC_URL;
const POLYMARKET_API_KEY = ENV.POLYMARKET_API_KEY?.trim();
const POLYMARKET_API_SECRET = ENV.POLYMARKET_API_SECRET?.trim();
const POLYMARKET_PASSPHRASE = ENV.POLYMARKET_PASSPHRASE?.trim();

const hasManualCreds = Boolean(
    POLYMARKET_API_KEY && POLYMARKET_API_SECRET && POLYMARKET_PASSPHRASE
);
const hasPartialCreds = Boolean(
    POLYMARKET_API_KEY || POLYMARKET_API_SECRET || POLYMARKET_PASSPHRASE
) && !hasManualCreds;

/**
 * Determines if a wallet is a Gnosis Safe by checking if it has contract code
 */
const isGnosisSafe = async (address: string): Promise<boolean> => {
    try {
        // Using ethers v5 syntax
        const provider = new ethers.providers.JsonRpcProvider(RPC_URL);
        const code = await provider.getCode(address);
        // If code is not "0x", then it's a contract (likely Gnosis Safe)
        return code !== '0x';
    } catch (error) {
        Logger.error(`Error checking wallet type: ${error}`);
        return false;
    }
};

const createClobClient = async (): Promise<ClobClient> => {
    const chainId = 137;
    const host = CLOB_HTTP_URL as string;
    const wallet = new ethers.Wallet(PRIVATE_KEY as string);

    // Detect if the proxy wallet is a Gnosis Safe or EOA
    const isProxySafe = await isGnosisSafe(PROXY_WALLET as string);
    const signatureType = isProxySafe ? SignatureType.POLY_GNOSIS_SAFE : SignatureType.EOA;

    Logger.info(
        `Wallet type detected: ${isProxySafe ? 'Gnosis Safe' : 'EOA (Externally Owned Account)'}`
    );

    if (hasPartialCreds) {
        Logger.warning('POLYMARKET_API_* is partially configured. Falling back to derived credentials.');
    }

    const envCreds = hasManualCreds
        ? {
              key: POLYMARKET_API_KEY as string,
              secret: POLYMARKET_API_SECRET as string,
              passphrase: POLYMARKET_PASSPHRASE as string,
          }
        : undefined;

    let clobClient = new ClobClient(
        host,
        chainId,
        wallet,
        envCreds,
        signatureType,
        isProxySafe ? (PROXY_WALLET as string) : undefined
    );

    if (envCreds) {
        Logger.info('Using Polymarket API credentials from environment.');
        return clobClient;
    }

    // Suppress console output during API key creation
    const originalConsoleLog = console.log;
    const originalConsoleError = console.error;
    console.log = function () {};
    console.error = function () {};

    let creds = await clobClient.createApiKey();
    if (!creds.key) {
        creds = await clobClient.deriveApiKey();
    }

    clobClient = new ClobClient(
        host,
        chainId,
        wallet,
        creds,
        signatureType,
        isProxySafe ? (PROXY_WALLET as string) : undefined
    );

    // Restore console functions
    console.log = originalConsoleLog;
    console.error = originalConsoleError;

    return clobClient;
};

export default createClobClient;
