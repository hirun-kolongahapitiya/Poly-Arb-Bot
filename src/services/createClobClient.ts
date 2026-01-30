import { ethers } from 'ethers';
import { ClobClient } from '@polymarket/clob-client';
import { SignatureType } from '@polymarket/order-utils';
import { ENV } from '../config/env';

const PROXY_WALLET = ENV.PROXY_WALLET;
const PRIVATE_KEY = ENV.PRIVATE_KEY;
const CLOB_HTTP_URL = ENV.CLOB_HTTP_URL;
const POLYMARKET_API_KEY = ENV.POLYMARKET_API_KEY?.trim();
const POLYMARKET_API_SECRET = ENV.POLYMARKET_API_SECRET?.trim();
const POLYMARKET_PASSPHRASE = ENV.POLYMARKET_PASSPHRASE?.trim();

const hasManualCreds = Boolean(
    POLYMARKET_API_KEY && POLYMARKET_API_SECRET && POLYMARKET_PASSPHRASE
);
const hasPartialCreds = Boolean(
    POLYMARKET_API_KEY || POLYMARKET_API_SECRET || POLYMARKET_PASSPHRASE
) && !hasManualCreds;

const createClobClient = async (): Promise<ClobClient> => {
    const chainId = 137;
    const host = CLOB_HTTP_URL as string;
    const wallet = new ethers.Wallet(PRIVATE_KEY as string);
    if (hasPartialCreds) {
        console.warn('POLYMARKET_API_* is partially configured. Falling back to derived credentials.');
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
        SignatureType.POLY_PROXY,
        PROXY_WALLET as string
    );

    if (envCreds) {
        console.log('Using Polymarket API credentials from environment.');
        return clobClient;
    }

    const originalConsoleError = console.error;
    console.error = function () {};
    let creds = await clobClient.createApiKey();
    console.error = originalConsoleError;
    if (creds.key) {
        console.log('API Key created', creds);
    } else {
        creds = await clobClient.deriveApiKey();
        console.log('API Key derived', creds);
    }

    clobClient = new ClobClient(
        host,
        chainId,
        wallet,
        creds,
        SignatureType.POLY_PROXY,
        PROXY_WALLET as string
    );
    console.log(clobClient);
    return clobClient;
};

export default createClobClient;
