import express, { Request, Response } from 'express';
import * as fs from 'fs';
import * as path from 'path';
import { randomUUID } from 'crypto';
import bcrypt from 'bcryptjs';
import axios from 'axios';
import { ClobClient, OrderType, Side } from '@polymarket/clob-client';
import Logger from './utils/logger';
import fetchData from './utils/fetchData';
import { startBot, stopBot, getBotStatus, getBotClient } from './bot';
import createClobClient from './utils/createClobClient';
import { performHealthCheck } from './utils/healthCheck';
import { ENV } from './config/env';
import connectDB from './config/db';
import { getUserActivityModel } from './models/userHistory';
import UserModel from './models/user';
import UserActivityLog from './models/userActivityLog';
import getMyBalance from './utils/getMyBalance';

const app = express();
app.use(express.json());

const HOST = process.env.SERVER_HOST || '127.0.0.1';
const PORT = parseInt(process.env.SERVER_PORT || '3000', 10);
const AUTO_START_BOT = (process.env.AUTO_START_BOT ?? 'false') === 'true';
const LOGIN_HASH = process.env.HASH || '';
const ANALYSIS_ONLY = (process.env.ANALYSIS_ONLY ?? 'true') === 'true';
const RECAPTCHA_ENABLED = (process.env.RECAPTCHA_ENABLED ?? 'true') === 'true';
const RECAPTCHA_SECRET_KEY = process.env.RECAPTCHA_SECRET_KEY || '';
const RECAPTCHA_MIN_SCORE = (() => {
    const raw = process.env.RECAPTCHA_MIN_SCORE;
    const parsed = raw ? parseFloat(raw) : 0.5;
    return Number.isFinite(parsed) ? parsed : 0.5;
})();

const dashboardDist = path.resolve(process.cwd(), 'dashboard', 'dist');
const dashboardIndex = path.join(dashboardDist, 'index.html');

type UserRole = 'admin' | 'user';

type AuthSession = {
    expiresAt: number;
    userId?: string;
    role?: UserRole;
    email?: string;
    legacy?: boolean;
};

type AuthRequest = Request & { auth?: AuthSession };

const TOKEN_TTL_MS = 12 * 60 * 60 * 1000;
const authTokens = new Map<string, AuthSession>();
let manualClient: ClobClient | null = null;
let clientPromise: Promise<ClobClient> | null = null;

const MIN_ORDER_SIZE_USD = 1.0;
const MIN_ORDER_SIZE_TOKENS = 1.0;

const MARKET_CACHE_MS = 30_000;
const MARKET_POOL_SIZE = 200;
const MARKET_SCAN_PAGE_SIZE = (() => {
    const raw = process.env.MARKET_SCAN_PAGE_SIZE;
    const parsed = raw ? parseInt(raw, 10) : 200;
    if (Number.isFinite(parsed) && parsed > 0) {
        return Math.min(parsed, 500);
    }
    return 200;
})();
const MARKET_SCAN_LIMIT = (() => {
    const raw = process.env.MARKET_SCAN_LIMIT;
    const parsed = raw ? parseInt(raw, 10) : 5000;
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return 5000;
})();
let marketCache: { fetchedAt: number; data: GammaMarket[] } = { fetchedAt: 0, data: [] };

const BINANCE_API = 'https://api.binance.com/api/v3';
const BINANCE_SYMBOLS = new Set(['BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'XRPUSDC']);
const BINANCE_INTERVALS = new Set(['1m', '5m', '15m', '1h', '4h', '1d']);

const LIFETIME_BATCH_SIZE = (() => {
    const raw = process.env.LIFETIME_BATCH_SIZE;
    const parsed = raw ? parseInt(raw, 10) : 500;
    if (Number.isFinite(parsed) && parsed > 0) {
        return Math.min(parsed, 500);
    }
    return 500;
})();
const LIFETIME_MAX_EVENTS = (() => {
    const raw = process.env.LIFETIME_MAX_EVENTS;
    const parsed = raw ? parseInt(raw, 10) : 100000;
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return 100000;
})();
const LIFETIME_CACHE_TTL_MS = 5 * 60 * 1000;
const ACTIVITY_REQUEST_LIMIT = (() => {
    const raw = process.env.ACTIVITY_REQUEST_LIMIT;
    const parsed = raw ? parseInt(raw, 10) : 500;
    if (Number.isFinite(parsed) && parsed > 0) {
        return Math.min(parsed, 500);
    }
    return 500;
})();
const ACTIVITY_RANGE_LOG = (process.env.ACTIVITY_RANGE_LOG ?? 'false') === 'true';
const ACTIVITY_RANGE_BATCH_SIZE = (() => {
    const raw = process.env.ACTIVITY_RANGE_BATCH_SIZE;
    const parsed = raw ? parseInt(raw, 10) : ACTIVITY_REQUEST_LIMIT;
    if (Number.isFinite(parsed) && parsed > 0) {
        return Math.min(parsed, 500);
    }
    return ACTIVITY_REQUEST_LIMIT;
})();
const ACTIVITY_RANGE_MAX_EVENTS = (() => {
    const raw = process.env.ACTIVITY_RANGE_MAX_EVENTS;
    const parsed = raw ? parseInt(raw, 10) : 5000;
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return 5000;
})();
const ACTIVITY_EARLIEST_MAX_EVENTS = (() => {
    const raw = process.env.ACTIVITY_EARLIEST_MAX_EVENTS;
    const parsed = raw ? parseInt(raw, 10) : 100000;
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return 100000;
})();
const lifetimeCache = new Map<
    string,
    {
        expiresAt: number;
        data: {
            totalProfit: number;
            totalLoss: number;
            netPnl: number;
            tradesCount: number;
            truncated: boolean;
            firstTradeAt: number | null;
            lastTradeAt: number | null;
            daily: Array<{
                date: string;
                net: number;
                profit: number;
                loss: number;
                events: number;
            }>;
            winRate: number | null;
            profitFactor: number | null;
            avgWinDay: number | null;
            avgLossDay: number | null;
            maxDrawdown: number | null;
            sharpeRatio: number | null;
            lastDayNet: number | null;
            activeDays: number;
        };
    }
>();

const getAuthToken = (req: Request): string | undefined => {
    const header = req.headers.authorization;
    if (!header) {
        return undefined;
    }
    const [type, token] = header.split(' ');
    if (type !== 'Bearer' || !token) {
        return undefined;
    }
    return token;
};

const getAuthSession = (token?: string): AuthSession | null => {
    if (!token) {
        return null;
    }
    const session = authTokens.get(token);
    if (!session) {
        return null;
    }
    if (Date.now() > session.expiresAt) {
        authTokens.delete(token);
        return null;
    }
    return session;
};

const requireAuth = (req: Request, res: Response, next: () => void) => {
    const session = getAuthSession(getAuthToken(req));
    if (!session) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }
    (req as AuthRequest).auth = session;
    next();
};

const requireUser = (req: Request, res: Response, next: () => void) => {
    const session = getAuthSession(getAuthToken(req));
    if (!session || !session.userId) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }
    (req as AuthRequest).auth = session;
    next();
};

const requireAdmin = (req: Request, res: Response, next: () => void) => {
    const session = getAuthSession(getAuthToken(req));
    if (!session) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }
    if (session.legacy || session.role === 'admin') {
        (req as AuthRequest).auth = session;
        next();
        return;
    }
    res.status(403).json({ error: 'Admin access required' });
};

const ensureDbConnection = async () => {
    await connectDB();
};

const ensureClobClient = async (): Promise<ClobClient> => {
    const existing = getBotClient();
    if (existing) {
        return existing;
    }

    if (manualClient) {
        return manualClient;
    }

    if (!clientPromise) {
        clientPromise = createClobClient().then((client) => {
            manualClient = client;
            return client;
        });
    }

    return clientPromise;
};

const parseNumber = (value: number | string | null | undefined): number => {
    if (value === null || value === undefined) {
        return 0;
    }
    if (typeof value === 'number') {
        return value;
    }
    const parsed = parseFloat(value);
    return Number.isNaN(parsed) ? 0 : parsed;
};

const normalizeEmail = (email: string): string => email.trim().toLowerCase();

const isValidEmail = (email: string): boolean =>
    /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

const isValidPassword = (password: string): boolean => password.length >= 8;

const isValidId = (value?: string): boolean =>
    typeof value === 'string' && /^[a-f0-9]{24}$/i.test(value);

const sanitizeUser = (user: any) => ({
    id: user?._id?.toString() ?? '',
    email: user?.email ?? '',
    role: user?.role ?? 'user',
    walletAddress: user?.walletAddress ?? '',
    createdAt: user?.createdAt ? new Date(user.createdAt).toISOString() : null,
    lastLoginAt: user?.lastLoginAt ? new Date(user.lastLoginAt).toISOString() : null,
});

const getRecaptchaToken = (req: Request): string =>
    typeof req.body?.recaptchaToken === 'string' ? req.body.recaptchaToken : '';

const verifyRecaptcha = async (
    token: string,
    expectedAction: string,
    remoteIp?: string
): Promise<{ ok: boolean; score: number | null; error?: string }> => {
    if (!RECAPTCHA_ENABLED) {
        return { ok: true, score: null };
    }
    if (!RECAPTCHA_SECRET_KEY) {
        return { ok: false, score: null, error: 'Recaptcha not configured' };
    }
    if (!token) {
        return { ok: false, score: null, error: 'Recaptcha token missing' };
    }

    try {
        const params = new URLSearchParams();
        params.set('secret', RECAPTCHA_SECRET_KEY);
        params.set('response', token);
        if (remoteIp) {
            params.set('remoteip', remoteIp);
        }

        const response = await axios.post(
            'https://www.google.com/recaptcha/api/siteverify',
            params.toString(),
            {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                timeout: 5000,
            }
        );

        const data = response.data as {
            success?: boolean;
            score?: number;
            action?: string;
            'error-codes'?: string[];
        };

        if (!data?.success) {
            return { ok: false, score: data?.score ?? null, error: 'Recaptcha failed' };
        }

        const action = typeof data.action === 'string' ? data.action : '';
        const score = typeof data.score === 'number' ? data.score : null;
        if (expectedAction && action && action !== expectedAction) {
            return { ok: false, score, error: 'Recaptcha action mismatch' };
        }
        if (score !== null && score < RECAPTCHA_MIN_SCORE) {
            return { ok: false, score, error: 'Recaptcha score too low' };
        }

        return { ok: true, score };
    } catch (error) {
        Logger.warning(`Recaptcha verification failed: ${error}`);
        return { ok: false, score: null, error: 'Recaptcha verification failed' };
    }
};

const logUserActivity = async (
    userId: string,
    action: string,
    metadata: Record<string, unknown> = {}
) => {
    try {
        await UserActivityLog.create({ userId, action, metadata });
    } catch (error) {
        Logger.warning(`Failed to log user activity (${action}): ${error}`);
    }
};

const normalizeSide = (side?: string): 'buy' | 'sell' =>
    side && side.toLowerCase() === 'sell' ? 'sell' : 'buy';

const getPositionKey = (trade: TradeActivity) => {
    if (trade.asset) {
        return trade.asset;
    }
    const label = trade.slug || trade.title || trade.eventSlug || 'unknown-market';
    const outcome = trade.outcome || 'unknown-outcome';
    return `${label}:${outcome}`;
};

const computePositionHistory = (trades: TradeActivity[]): PositionHistoryEntry[] => {
    const sorted = trades
        .map((trade, index) => ({
            trade,
            index,
            timestamp: normalizeTimestampMs(parseNumber(trade.timestamp)),
        }))
        .sort((a, b) => a.timestamp - b.timestamp || a.index - b.index)
        .map((entry) => entry.trade);

    const positions = new Map<
        string,
        {
            size: number;
            costBasisUsd: number;
            avgPrice: number;
            realizedPnl: number;
            asset: string;
            market: string;
            slug?: string;
            outcome: string;
        }
    >();
    const history: PositionHistoryEntry[] = [];

    for (const trade of sorted) {
        const side = normalizeSide(trade.side);
        let tradePrice = parseNumber(trade.price);
        const tradeUsdc = parseNumber(trade.usdcSize);
        let tradeSize = parseNumber(trade.size);
        if (!tradeSize && tradePrice > 0 && tradeUsdc > 0) {
            tradeSize = tradeUsdc / tradePrice;
        }
        if (!tradePrice && tradeSize > 0 && tradeUsdc > 0) {
            tradePrice = tradeUsdc / tradeSize;
        }

        const key = getPositionKey(trade);
        const existing = positions.get(key) || {
            size: 0,
            costBasisUsd: 0,
            avgPrice: 0,
            realizedPnl: 0,
            asset: trade.asset || '',
            market: trade.title || trade.market || trade.slug || trade.eventSlug || '',
            slug: trade.slug || trade.eventSlug || undefined,
            outcome: trade.outcome || '',
        };

        let realizedDelta = 0;
        let nextSize = existing.size;
        let nextCostBasis = existing.costBasisUsd;

        if (side === 'sell') {
            if (tradeSize > 0 && existing.size > 0) {
                const avgCost = existing.costBasisUsd / existing.size;
                realizedDelta = tradeUsdc - avgCost * tradeSize;
                nextSize = existing.size - tradeSize;
                nextCostBasis = existing.costBasisUsd - avgCost * tradeSize;
            }
        } else {
            nextSize = existing.size + tradeSize;
            nextCostBasis = existing.costBasisUsd + tradeUsdc;
        }

        if (nextSize <= 0) {
            nextSize = 0;
            nextCostBasis = 0;
        }

        const nextAvgPrice = nextSize !== 0 ? Math.abs(nextCostBasis / nextSize) : 0;
        const nextRealized = existing.realizedPnl + realizedDelta;
        const status = nextSize === 0 ? 'CLOSED' : 'OPEN';

        const timestamp = parseNumber(trade.timestamp);
        history.push({
            key,
            asset: existing.asset || trade.asset || '',
            market: existing.market || trade.title || trade.market || trade.slug || trade.eventSlug || '',
            slug: existing.slug || trade.slug || trade.eventSlug || undefined,
            outcome: existing.outcome || trade.outcome || '',
            side,
            tradePrice,
            tradeSize,
            tradeUsdc,
            timestamp,
            positionSize: nextSize,
            positionCostBasis: nextCostBasis,
            positionAvgPrice: nextAvgPrice,
            realizedPnl: realizedDelta,
            cumulativeRealizedPnl: nextRealized,
            status,
        });

        positions.set(key, {
            size: nextSize,
            costBasisUsd: nextCostBasis,
            avgPrice: nextAvgPrice,
            realizedPnl: nextRealized,
            asset: existing.asset || trade.asset || '',
            market: existing.market || trade.title || trade.market || trade.slug || trade.eventSlug || '',
            slug: existing.slug || trade.slug || trade.eventSlug || undefined,
            outcome: existing.outcome || trade.outcome || '',
        });
    }

    return history;
};

const computePositionSummary = (trades: TradeActivity[]): PositionSummaryEntry[] => {
    const sorted = trades
        .map((trade, index) => ({
            trade,
            index,
            timestamp: normalizeTimestampMs(parseNumber(trade.timestamp)),
        }))
        .sort((a, b) => a.timestamp - b.timestamp || a.index - b.index)
        .map((entry) => entry.trade);

    const positions = new Map<
        string,
        {
            key: string;
            asset: string;
            market: string;
            slug?: string;
            outcome: string;
            buySize: number;
            buyCost: number;
            sellSize: number;
            sellProceeds: number;
            openSize: number;
            costBasisUsd: number;
            realizedPnl: number;
            lastPrice: number | null;
            lastTradeAt: number | null;
        }
    >();

    for (const trade of sorted) {
        const side = normalizeSide(trade.side);
        let tradePrice = parseNumber(trade.price);
        const tradeUsdc = parseNumber(trade.usdcSize);
        let tradeSize = parseNumber(trade.size);
        if (!tradeSize && tradePrice > 0 && tradeUsdc > 0) {
            tradeSize = tradeUsdc / tradePrice;
        }
        if (!tradePrice && tradeSize > 0 && tradeUsdc > 0) {
            tradePrice = tradeUsdc / tradeSize;
        }

        const key = getPositionKey(trade);
        const existing = positions.get(key) || {
            key,
            asset: trade.asset || '',
            market: trade.title || trade.market || trade.slug || trade.eventSlug || '',
            slug: trade.slug || trade.eventSlug || undefined,
            outcome: trade.outcome || '',
            buySize: 0,
            buyCost: 0,
            sellSize: 0,
            sellProceeds: 0,
            openSize: 0,
            costBasisUsd: 0,
            realizedPnl: 0,
            lastPrice: null,
            lastTradeAt: null,
        };

        if (tradePrice > 0) {
            existing.lastPrice = tradePrice;
        }
        const timestamp = parseNumber(trade.timestamp);
        if (timestamp > 0) {
            existing.lastTradeAt = timestamp;
        }

        if (side === 'buy') {
            existing.buySize += tradeSize;
            existing.buyCost += tradeUsdc;
            existing.openSize += tradeSize;
            existing.costBasisUsd += tradeUsdc;
        } else {
            existing.sellSize += tradeSize;
            existing.sellProceeds += tradeUsdc;
            if (existing.openSize > 0 && tradeSize > 0) {
                const avgCost = existing.costBasisUsd / existing.openSize;
                existing.realizedPnl += tradeUsdc - avgCost * tradeSize;
                existing.openSize -= tradeSize;
                existing.costBasisUsd -= avgCost * tradeSize;
                if (existing.openSize <= 0) {
                    existing.openSize = 0;
                    existing.costBasisUsd = 0;
                }
            }
        }

        positions.set(key, existing);
    }

    return Array.from(positions.values()).map((pos) => {
        const avgEntry = pos.buySize > 0 ? pos.buyCost / pos.buySize : 0;
        const avgExit = pos.sellSize > 0 ? pos.sellProceeds / pos.sellSize : 0;
        const unrealized =
            pos.openSize > 0 && pos.lastPrice
                ? pos.lastPrice * pos.openSize - pos.costBasisUsd
                : null;
        const totalPnl = pos.realizedPnl + (unrealized ?? 0);
        return {
            key: pos.key,
            asset: pos.asset,
            market: pos.market,
            slug: pos.slug,
            outcome: pos.outcome,
            buySize: pos.buySize,
            buyCost: pos.buyCost,
            avgEntry,
            sellSize: pos.sellSize,
            sellProceeds: pos.sellProceeds,
            avgExit,
            realizedPnl: pos.realizedPnl,
            unrealizedPnl: unrealized,
            totalPnl,
            openSize: pos.openSize,
            lastPrice: pos.lastPrice,
            lastTradeAt: pos.lastTradeAt,
            status: pos.openSize > 0 ? 'OPEN' : 'CLOSED',
        };
    });
};

const computeTradeDailyPnl = (trades: TradeActivity[]): DailyTradePnlEntry[] => {
    const daily = new Map<string, DailyTradePnlEntry>();
    trades.forEach((trade) => {
        const timestamp = parseNumber(trade.timestamp);
        if (!timestamp) {
            return;
        }
        const net = normalizeSide(trade.side) === 'sell' ? parseNumber(trade.usdcSize) : -parseNumber(trade.usdcSize);
        if (!net) {
            return;
        }
        const date = new Date(normalizeTimestampMs(timestamp)).toISOString().slice(0, 10);
        const existing = daily.get(date) || { date, net: 0, profit: 0, loss: 0, trades: 0 };
        existing.net += net;
        if (net > 0) {
            existing.profit += net;
        } else {
            existing.loss += Math.abs(net);
        }
        existing.trades += 1;
        daily.set(date, existing);
    });
    return Array.from(daily.values()).sort((a, b) => a.date.localeCompare(b.date));
};

const buildTraderProfileSummary = (
    positions: PositionSummaryEntry[],
    balance: number | null,
    positionsValue: number | null,
    portfolioValue: number | null,
    totalTrades: number
): TraderProfileSummary => {
    let realizedPnl = 0;
    let unrealizedPnl = 0;
    let wins = 0;
    let losses = 0;
    let closedPositions = 0;
    let openPositions = 0;

    positions.forEach((position) => {
        realizedPnl += position.realizedPnl;
        if (position.unrealizedPnl !== null && Number.isFinite(position.unrealizedPnl)) {
            unrealizedPnl += position.unrealizedPnl;
        }
        if (position.status === 'OPEN') {
            openPositions += 1;
        } else {
            closedPositions += 1;
            if (position.realizedPnl > 0) {
                wins += 1;
            } else if (position.realizedPnl < 0) {
                losses += 1;
            }
        }
    });

    const winRate = closedPositions > 0 ? wins / closedPositions : null;
    const totalPnl = realizedPnl + unrealizedPnl;

    return {
        totalTrades,
        wins,
        losses,
        winRate,
        realizedPnl,
        unrealizedPnl,
        totalPnl,
        openPositions,
        closedPositions,
        balance,
        positionsValue,
        portfolioValue,
    };
};

const buildCategorySummary = (
    positions: PositionSummaryEntry[],
    categories: Map<string, MarketCategoryInfo | null>
): CategorySummaryEntry[] => {
    const summary = new Map<string, CategorySummaryEntry>();

    positions.forEach((position) => {
        const slug = position.slug ?? '';
        const info = slug ? categories.get(slug) ?? null : null;
        const category = info?.category || 'Uncategorized';
        const entry = summary.get(category) || {
            category,
            realizedPnl: 0,
            unrealizedPnl: 0,
            totalPnl: 0,
            tradeCount: 0,
            winCount: 0,
            lossCount: 0,
        };
        entry.realizedPnl += position.realizedPnl;
        if (position.unrealizedPnl !== null && Number.isFinite(position.unrealizedPnl)) {
            entry.unrealizedPnl += position.unrealizedPnl;
        }
        entry.totalPnl = entry.realizedPnl + entry.unrealizedPnl;
        entry.tradeCount += position.buySize + position.sellSize > 0 ? 1 : 0;
        if (position.status === 'CLOSED') {
            if (position.realizedPnl > 0) {
                entry.winCount += 1;
            } else if (position.realizedPnl < 0) {
                entry.lossCount += 1;
            }
        }
        summary.set(category, entry);
    });

    return Array.from(summary.values()).sort((a, b) => b.totalPnl - a.totalPnl);
};

const fetchWalletSummaryData = async (user: string) => {
    const positions = await fetchData(`https://data-api.polymarket.com/positions?user=${user}`);
    const list = Array.isArray(positions) ? positions : [];
    let positionsValue = 0;
    let weightedPnl = 0;

    list.forEach((position: any) => {
        const value = parseNumber(position.currentValue || position.value);
        positionsValue += value;
        const pnl = parseNumber(position.pnl || position.unrealizedPnl || position.unrealizedPnlUsd);
        if (value > 0) {
            weightedPnl += pnl * value;
        }
    });

    const balance = await getMyBalance(user);
    const portfolioValue = positionsValue + balance;
    const pnlPercent = positionsValue > 0 ? weightedPnl / positionsValue : 0;

    return {
        balance,
        positionsValue,
        portfolioValue,
        pnlPercent,
        positionsCount: list.length,
    };
};

const ACTIVITY_POSITIVE_TYPES = new Set([
    'REDEEM',
    'SETTLEMENT',
    'SETTLE',
    'CLAIM',
    'PAYOUT',
    'REWARD',
    'MAKER_REBATE',
    'REBATE',
    'REFUND',
    'CREDIT',
]);
const ACTIVITY_NEGATIVE_TYPES = new Set([
    'FEE',
    'MAKER_FEE',
    'TAKER_FEE',
    'PENALTY',
    'DEBIT',
]);
const ACTIVITY_IGNORE_TYPES = new Set(['TRANSFER', 'DEPOSIT', 'WITHDRAWAL', 'WITHDRAW']);

const normalizeActivityType = (value: unknown): string =>
    typeof value === 'string' ? value.toUpperCase() : '';

const isTradeActivity = (activity: any): boolean =>
    normalizeActivityType(activity?.type) === 'TRADE';

const normalizeTimestampMs = (timestamp: number): number =>
    timestamp < 1_000_000_000_000 ? timestamp * 1000 : timestamp;

const normalizeTimestampSeconds = (timestamp: number): number =>
    timestamp >= 1_000_000_000_000 ? Math.floor(timestamp / 1000) : Math.floor(timestamp);

const parseTimestampInput = (value: unknown): number | null => {
    if (Array.isArray(value)) {
        return parseTimestampInput(value[0]);
    }
    if (value === null || value === undefined) {
        return null;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
        return normalizeTimestampSeconds(value);
    }
    if (typeof value !== 'string') {
        return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return null;
    }
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
        return normalizeTimestampSeconds(numeric);
    }
    const parsedDate = Date.parse(trimmed);
    if (!Number.isNaN(parsedDate)) {
        return Math.floor(parsedDate / 1000);
    }
    const isoNoZone = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(:\d{2})?$/;
    const dateOnly = /^(\d{4}-\d{2}-\d{2})$/;
    let normalized: string | null = null;
    const isoMatch = trimmed.match(isoNoZone);
    if (isoMatch) {
        const seconds = isoMatch[3] ? isoMatch[3] : ':00';
        normalized = `${isoMatch[1]}T${isoMatch[2]}${seconds}Z`;
    } else {
        const dateMatch = trimmed.match(dateOnly);
        if (dateMatch) {
            normalized = `${dateMatch[1]}T00:00:00Z`;
        }
    }
    if (normalized) {
        const normalizedParsed = Date.parse(normalized);
        if (!Number.isNaN(normalizedParsed)) {
            return Math.floor(normalizedParsed / 1000);
        }
    }
    return null;
};

const parseDateToSeconds = (value: string | null | undefined): number | null => {
    if (!value) {
        return null;
    }
    const parsed = Date.parse(value);
    if (Number.isNaN(parsed)) {
        return null;
    }
    return Math.floor(parsed / 1000);
};

type EventPriceSnapshot = {
    priceToBeat: number | null;
    currentPrice: number | null;
};

const parseOptionalNumber = (value: unknown): number | null => {
    if (value === null || value === undefined) {
        return null;
    }
    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : null;
    }
    const parsed = parseFloat(String(value));
    return Number.isFinite(parsed) ? parsed : null;
};

const extractEventPrices = (payload: unknown): EventPriceSnapshot => {
    const empty: EventPriceSnapshot = { priceToBeat: null, currentPrice: null };
    if (!payload || typeof payload !== 'object') {
        return empty;
    }
    const queries = (payload as Record<string, any>)?.props?.pageProps?.dehydratedState?.queries;
    if (!Array.isArray(queries)) {
        return empty;
    }
    for (const query of queries) {
        const key = query?.queryKey;
        if (Array.isArray(key) && key[0] === 'crypto-prices') {
            const data = query?.state?.data;
            if (data && typeof data === 'object') {
                return {
                    priceToBeat: parseOptionalNumber((data as Record<string, unknown>).openPrice),
                    currentPrice: parseOptionalNumber((data as Record<string, unknown>).closePrice),
                };
            }
        }
    }
    return empty;
};

const fetchEventPricesForSlug = async (slug: string): Promise<EventPriceSnapshot> => {
    const empty: EventPriceSnapshot = { priceToBeat: null, currentPrice: null };
    if (!slug) {
        return empty;
    }
    try {
        const url = `https://polymarket.com/event/${slug}`;
        const html = await fetchData(url);
        if (typeof html !== 'string') {
            return empty;
        }
        const match = html.match(
            /id="__NEXT_DATA__" type="application\/json"[^>]*>([\s\S]*?)<\/script>/
        );
        if (!match) {
            return empty;
        }
        let data: unknown;
        try {
            data = JSON.parse(match[1]);
        } catch {
            return empty;
        }
        return extractEventPrices(data);
    } catch {
        return empty;
    }
};

const parseBookLevels = (raw: unknown): Array<{ price: number; size: number }> => {
    if (!Array.isArray(raw)) {
        return [];
    }
    const levels: Array<{ price: number; size: number }> = [];
    raw.forEach((level) => {
        let price: unknown;
        let size: unknown;
        if (Array.isArray(level) && level.length >= 2) {
            [price, size] = level;
        } else if (level && typeof level === 'object') {
            const record = level as Record<string, unknown>;
            price = record.price ?? record.p;
            size = record.size ?? record.s ?? record.amount;
        }
        const parsedPrice = parseOptionalNumber(price);
        const parsedSize = parseOptionalNumber(size);
        if (parsedPrice === null || parsedSize === null) {
            return;
        }
        levels.push({ price: parsedPrice, size: parsedSize });
    });
    return levels;
};

const extractBookSnapshot = (book: unknown) => {
    if (!book || typeof book !== 'object') {
        return {
            bids: [] as Array<[number, number]>,
            asks: [] as Array<[number, number]>,
            bestBid: null,
            bestAsk: null,
            mid: null,
            spread: null,
        };
    }
    const record = book as Record<string, unknown>;
    const bids = parseBookLevels(record.bids ?? record.bid ?? record.b ?? []);
    const asks = parseBookLevels(record.asks ?? record.ask ?? record.a ?? []);
    const bestBid = bids.length ? Math.max(...bids.map((level) => level.price)) : null;
    const bestAsk = asks.length ? Math.min(...asks.map((level) => level.price)) : null;
    const mid = bestBid !== null && bestAsk !== null ? (bestBid + bestAsk) / 2 : null;
    const spread = bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null;
    return {
        bids: bids.map((level) => [level.price, level.size]),
        asks: asks.map((level) => [level.price, level.size]),
        bestBid,
        bestAsk,
        mid,
        spread,
    };
};

const computeOrderBookLiquidity = (book: unknown): number | null => {
    if (!book || typeof book !== 'object') {
        return null;
    }
    const record = book as Record<string, unknown>;
    const bids = parseBookLevels(record.bids ?? record.bid ?? []);
    const asks = parseBookLevels(record.asks ?? record.ask ?? []);
    if (bids.length === 0 && asks.length === 0) {
        return null;
    }
    const bidNotional = bids.reduce((sum, level) => sum + level.price * level.size, 0);
    const askNotional = asks.reduce((sum, level) => sum + level.price * level.size, 0);
    const totalNotional = bidNotional + askNotional;
    return Number.isFinite(totalNotional) ? totalNotional : null;
};

const fetchOrderBookLiquidity = async (tokenId: string): Promise<number | null> => {
    if (!tokenId) {
        return null;
    }
    const base = ENV.CLOB_HTTP_URL?.replace(/\/$/, '');
    if (!base) {
        return null;
    }
    const url = `${base}/book?token_id=${encodeURIComponent(tokenId)}`;
    try {
        const book = await fetchData(url);
        return computeOrderBookLiquidity(book);
    } catch {
        return null;
    }
};

const parseSlugTimestamp = (slug: string): number | null => {
    const match = slug.match(/-(\d{9,})$/);
    if (!match) {
        return null;
    }
    const raw = parseInt(match[1], 10);
    if (!Number.isFinite(raw)) {
        return null;
    }
    return normalizeTimestampSeconds(raw);
};

const stripSlugTimestamp = (slug: string): string => slug.replace(/-\d+$/, '');

const parsePrefixIntervalSeconds = (prefix: string): number | null => {
    const cleaned = stripSlugTimestamp(prefix);
    const match = cleaned.match(/-(\d+)([mhd])$/);
    if (!match) {
        return null;
    }
    const amount = parseInt(match[1], 10);
    if (!Number.isFinite(amount) || amount <= 0) {
        return null;
    }
    switch (match[2]) {
        case 'm':
            return amount * 60;
        case 'h':
            return amount * 60 * 60;
        case 'd':
            return amount * 60 * 60 * 24;
        default:
            return null;
    }
};

const alignTimestampToInterval = (
    timestamp: number,
    intervalSeconds: number,
    direction: 'floor' | 'ceil'
): number => {
    if (intervalSeconds <= 0) {
        return timestamp;
    }
    const ratio = timestamp / intervalSeconds;
    const aligned =
        direction === 'ceil' ? Math.ceil(ratio) * intervalSeconds : Math.floor(ratio) * intervalSeconds;
    return aligned;
};

const isQueryValuePresent = (value: unknown): boolean => {
    if (value === undefined || value === null) {
        return false;
    }
    if (Array.isArray(value)) {
        return value.some((entry) => isQueryValuePresent(entry));
    }
    if (typeof value === 'string') {
        return value.trim().length > 0;
    }
    return true;
};

const isTimestampInRange = (
    timestamp: number | null,
    startTs?: number | null,
    endTs?: number | null
): boolean => {
    if (timestamp === null || Number.isNaN(timestamp)) {
        return false;
    }
    if (startTs !== null && startTs !== undefined && timestamp < startTs) {
        return false;
    }
    if (endTs !== null && endTs !== undefined && timestamp > endTs) {
        return false;
    }
    return true;
};

const getActivityAmount = (item: any): number => {
    const value =
        item.usdcSize ??
        item.amount ??
        item.value ??
        item.usdcAmount ??
        item.amountUsd ??
        item.cashDelta ??
        item.delta ??
        item.pnl;
    return parseNumber(value);
};

const isIgnoredActivityType = (type: string): boolean => {
    if (ACTIVITY_IGNORE_TYPES.has(type)) {
        return true;
    }
    return (
        type.includes('TRANSFER') ||
        type.includes('WITHDRAW') ||
        type.includes('DEPOSIT') ||
        type.includes('BRIDGE')
    );
};

const isPositiveActivityType = (type: string): boolean => {
    if (ACTIVITY_POSITIVE_TYPES.has(type)) {
        return true;
    }
    return (
        type.includes('REDEEM') ||
        type.includes('REBATE') ||
        type.includes('REWARD') ||
        type.includes('PAYOUT') ||
        type.includes('CLAIM') ||
        type.includes('SETTLE') ||
        type.includes('CREDIT')
    );
};

const isNegativeActivityType = (type: string): boolean => {
    if (ACTIVITY_NEGATIVE_TYPES.has(type)) {
        return true;
    }
    return type.includes('FEE') || type.includes('PENALTY') || type.includes('DEBIT');
};

const getActivityNetDelta = (activity: any): number => {
    const amountRaw = getActivityAmount(activity);
    if (!amountRaw) {
        return 0;
    }
    if (amountRaw < 0) {
        return amountRaw;
    }

    const type = normalizeActivityType(activity.type);
    if (type === 'TRADE') {
        if (typeof activity.side === 'string') {
            const side = normalizeSide(activity.side);
            return side === 'buy' ? -amountRaw : amountRaw;
        }
        return 0;
    }

    if (typeof activity.side === 'string') {
        const side = normalizeSide(activity.side);
        return side === 'buy' ? -amountRaw : amountRaw;
    }

    if (isIgnoredActivityType(type)) {
        return 0;
    }

    if (isPositiveActivityType(type)) {
        return amountRaw;
    }
    if (isNegativeActivityType(type)) {
        return -amountRaw;
    }

    return 0;
};

const computeLifetimeAnalytics = (activities: any[]) => {
    const sorted = activities
        .map((activity, index) => ({
            activity,
            index,
            timestamp: parseNumber(activity.timestamp),
        }))
        .sort((a, b) => a.timestamp - b.timestamp || a.index - b.index)
        .map((entry) => entry.activity);

    let totalProfit = 0;
    let totalLoss = 0;
    let netPnl = 0;
    const dailyMap = new Map<
        string,
        { date: string; net: number; profit: number; loss: number; events: number }
    >();

    for (const activity of sorted) {
        const delta = getActivityNetDelta(activity);
        if (!delta) {
            continue;
        }

        netPnl += delta;
        if (delta > 0) {
            totalProfit += delta;
        } else {
            totalLoss += Math.abs(delta);
        }

        const timestamp = parseNumber(activity.timestamp);
        if (timestamp > 0) {
            const date = new Date(normalizeTimestampMs(timestamp)).toISOString().slice(0, 10);
            const existing = dailyMap.get(date) || {
                date,
                net: 0,
                profit: 0,
                loss: 0,
                events: 0,
            };
            existing.net += delta;
            if (delta > 0) {
                existing.profit += delta;
            } else {
                existing.loss += Math.abs(delta);
            }
            existing.events += 1;
            dailyMap.set(date, existing);
        }
    }

    const daily = Array.from(dailyMap.values()).sort((a, b) => a.date.localeCompare(b.date));
    const activeDays = daily.length;
    const totalPositive = daily.reduce((sum, day) => sum + (day.net > 0 ? day.net : 0), 0);
    const totalNegative = daily.reduce((sum, day) => sum + (day.net < 0 ? -day.net : 0), 0);
    const winDays = daily.filter((day) => day.net > 0).length;
    const lossDays = daily.filter((day) => day.net < 0).length;
    const winRate = activeDays > 0 ? winDays / activeDays : null;
    const profitFactor = totalNegative > 0 ? totalPositive / totalNegative : null;
    const avgWinDay = winDays > 0 ? totalPositive / winDays : null;
    const avgLossDay = lossDays > 0 ? totalNegative / lossDays : null;

    let cumulative = 0;
    let peak = 0;
    let maxDrawdown = 0;
    for (const day of daily) {
        cumulative += day.net;
        if (cumulative > peak) {
            peak = cumulative;
        }
        const drawdown = peak - cumulative;
        if (drawdown > maxDrawdown) {
            maxDrawdown = drawdown;
        }
    }
    const maxDrawdownValue = activeDays > 0 ? maxDrawdown : null;

    let sharpeRatio: number | null = null;
    if (activeDays >= 2) {
        const mean = daily.reduce((sum, day) => sum + day.net, 0) / activeDays;
        const variance =
            daily.reduce((sum, day) => sum + Math.pow(day.net - mean, 2), 0) /
            (activeDays - 1);
        const stdev = Math.sqrt(variance);
        if (stdev > 0) {
            sharpeRatio = (mean / stdev) * Math.sqrt(365);
        }
    }

    const firstTrade = sorted.find((activity) => parseNumber(activity.timestamp) > 0);
    const lastTrade = [...sorted].reverse().find((activity) => parseNumber(activity.timestamp) > 0);
    const lastDayNet = daily.length > 0 ? daily[daily.length - 1].net : null;

    return {
        totalProfit,
        totalLoss,
        netPnl,
        daily,
        winRate,
        profitFactor,
        avgWinDay,
        avgLossDay,
        maxDrawdown: maxDrawdownValue,
        sharpeRatio,
        lastDayNet,
        activeDays,
        firstTradeAt: firstTrade ? parseNumber(firstTrade.timestamp) : null,
        lastTradeAt: lastTrade ? parseNumber(lastTrade.timestamp) : null,
    };
};

const isValidEthereumAddress = (address: string): boolean => {
    return /^0x[a-fA-F0-9]{40}$/.test(address);
};

const isValidBinanceSymbol = (symbol: string): boolean => {
    return BINANCE_SYMBOLS.has(symbol);
};

const isValidBinanceInterval = (interval: string): boolean => {
    return BINANCE_INTERVALS.has(interval);
};

const getLogFileName = () => {
    const date = new Date().toISOString().split('T')[0];
    return path.join(process.cwd(), 'logs', `bot-${date}.log`);
};

const readLogLines = (limit: number) => {
    const logFile = getLogFileName();
    if (!fs.existsSync(logFile)) {
        return [];
    }

    const content = fs.readFileSync(logFile, 'utf8');
    if (!content.trim()) {
        return [];
    }

    const lines = content.split(/\r?\n/);
    return lines.slice(-limit);
};

type GammaMarket = {
    id?: number;
    question?: string | null;
    slug?: string | null;
    series_id?: number | string | null;
    seriesId?: number | string | null;
    endDate?: string | null;
    endDateIso?: string | null;
    volume24hr?: number | string | null;
    volume1wk?: number | string | null;
    volume1mo?: number | string | null;
    volume?: number | string | null;
    liquidity?: number | string | null;
    liquidityNum?: number | string | null;
    lastTradePrice?: number | string | null;
    outcomes?: string[] | null;
    outcomePrices?: Array<number | string> | null;
    icon?: string | null;
    image?: string | null;
    updatedAt?: string | null;
    clobTokenIds?: string[] | null;
    conditionId?: string | null;
    condition_id?: string | null;
};

type MarketSummary = {
    id: number;
    question: string;
    slug?: string;
    endDate?: string;
    volume24hr: number;
    liquidity: number;
    lastTradePrice: number;
    outcomes: string[];
    outcomePrices: number[];
    clobTokenIds: string[];
    icon?: string;
};

type MarketSearchEntry = {
    id: number;
    question: string;
    slug?: string;
    category?: string | null;
    tags: string[];
    endDate?: string | null;
    volume: number;
    openInterest: number | null;
    liquidity: number;
    description?: string | null;
    outcomes: string[];
    outcomePrices: number[];
    clobTokenIds: string[];
    conditionId?: string;
    active?: boolean;
    closed?: boolean;
    source: 'Polymarket';
};

type MarketSeriesEntry = {
    id: number;
    question: string;
    slug?: string;
    conditionId?: string;
    endDate?: string | null;
    timestamp?: number | null;
    outcomes: string[];
    clobTokenIds: string[];
};

type MarketTradeEntry = {
    id: string;
    market: string;
    owner?: string;
    side?: string;
    outcome?: string;
    outcomeIndex?: number;
    price?: number;
    size?: number;
    usdcSize?: number;
    timestamp?: number;
    transactionHash?: string;
};

const getMarketPool = async (): Promise<GammaMarket[]> => {
    const now = Date.now();
    if (now - marketCache.fetchedAt < MARKET_CACHE_MS && marketCache.data.length > 0) {
        return marketCache.data;
    }

    const url = `https://gamma-api.polymarket.com/markets?active=true&limit=${MARKET_POOL_SIZE}`;
    const data = await fetchData(url);
    if (!Array.isArray(data)) {
        throw new Error('Gamma API returned unexpected response');
    }

    marketCache = { fetchedAt: now, data };
    return data;
};

const mapMarket = (market: GammaMarket): MarketSummary => {
    const outcomes = Array.isArray(market.outcomes) ? market.outcomes : [];
    const outcomePricesRaw = Array.isArray(market.outcomePrices) ? market.outcomePrices : [];
    const outcomePrices = outcomePricesRaw.map((price) => parseNumber(price));
    const clobTokenIds = Array.isArray(market.clobTokenIds) ? market.clobTokenIds : [];

    return {
        id: market.id ?? 0,
        question: market.question ?? 'Untitled market',
        slug: market.slug ?? undefined,
        endDate: market.endDateIso ?? market.endDate ?? undefined,
        volume24hr: parseNumber(market.volume24hr || market.volume1wk || market.volume1mo || market.volume),
        liquidity: parseNumber(market.liquidity || market.liquidityNum),
        lastTradePrice: parseNumber(market.lastTradePrice),
        outcomes,
        outcomePrices,
        clobTokenIds,
        icon: market.icon ?? market.image ?? undefined,
    };
};

const mapMarketSearchEntry = (market: any): MarketSearchEntry => {
    const outcomes = parseStringArray(market.outcomes);
    const outcomePricesRaw = parseStringArray(market.outcomePrices);
    const outcomePrices = outcomePricesRaw.map((price) => parseNumber(price));
    const clobTokenIds = parseStringArray(market.clobTokenIds);
    const category = (market.category ?? market?.events?.[0]?.category ?? null) as string | null;
    const tagsRaw = Array.isArray(market?.tags) ? market.tags : [];
    const tags = tagsRaw.map((tag: unknown) => String(tag)).filter((tag: string) => tag.length > 0);
    if (category && !tags.includes(category)) {
        tags.unshift(category);
    }
    const endDate =
        (market.endDateIso ?? market.endDate ?? market?.events?.[0]?.endDate ?? null) as
            | string
            | null;
    const description =
        (market.description ?? market?.events?.[0]?.description ?? null) as string | null;
    const openInterestRaw = market.openInterest ?? market?.events?.[0]?.openInterest ?? null;
    const openInterest = parseOptionalNumber(openInterestRaw);
    return {
        id: parseInt(String(market.id ?? 0), 10) || 0,
        question: market.question ?? market?.events?.[0]?.title ?? 'Untitled market',
        slug: market.slug ?? undefined,
        category,
        tags,
        endDate,
        volume: parseNumber(market.volume24hr || market.volume || market.volumeNum),
        openInterest,
        liquidity: parseNumber(market.liquidity || market.liquidityNum),
        description,
        outcomes,
        outcomePrices,
        clobTokenIds,
        conditionId: market.conditionId ?? market.condition_id ?? undefined,
        active: market.active ?? undefined,
        closed: market.closed ?? undefined,
        source: 'Polymarket',
    };
};

type MarketTokenChoice = {
    tokenId: string;
    outcome?: string;
    outcomeIndex?: number;
};

const normalizeTokenIds = (value: unknown): string[] => {
    return parseStringArray(value).filter((tokenId) => Boolean(tokenId));
};

const parseStringArray = (value: unknown): string[] => {
    if (Array.isArray(value)) {
        return value.map((item) => String(item ?? '').trim()).filter((item) => item.length > 0);
    }
    if (typeof value === 'string') {
        const trimmed = value.trim();
        if (!trimmed) {
            return [];
        }
        if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
            try {
                const parsed = JSON.parse(trimmed);
                if (Array.isArray(parsed)) {
                    return parsed
                        .map((item) => String(item ?? '').trim())
                        .filter((item) => item.length > 0);
                }
            } catch {
                return [];
            }
        }
        return trimmed
            .split(',')
            .map((item) => item.trim())
            .filter((item) => item.length > 0);
    }
    return [];
};

const normalizeOutcomes = (value: unknown): string[] => {
    return parseStringArray(value);
};

const fetchGammaMarketsBySlug = async (slug: string): Promise<GammaMarket[]> => {
    const url = `https://gamma-api.polymarket.com/markets?slug=${encodeURIComponent(slug)}`;
    const data = await fetchData(url);
    if (Array.isArray(data)) {
        return data;
    }
    if (data && typeof data === 'object') {
        const payload = data as { data?: GammaMarket[]; markets?: GammaMarket[] };
        if (Array.isArray(payload.data)) {
            return payload.data;
        }
        if (Array.isArray(payload.markets)) {
            return payload.markets;
        }
        const single = data as GammaMarket;
        if (
            typeof single.slug === 'string' ||
            typeof single.id === 'number' ||
            typeof single.id === 'string'
        ) {
            return [single];
        }
    }
    return [];
};

type MarketCategoryInfo = {
    slug: string;
    question: string | null;
    category: string | null;
    tags: string[];
    endDate: string | null;
    openInterest: number | null;
    volume: number | null;
};

const marketCategoryCache = new Map<string, MarketCategoryInfo>();

const extractMarketCategoryInfo = (market: any, slug: string): MarketCategoryInfo => {
    const category = (market?.category ?? market?.events?.[0]?.category ?? null) as string | null;
    const tagsRaw = Array.isArray(market?.tags) ? market.tags : [];
    const tags = tagsRaw.map((tag: unknown) => String(tag)).filter((tag: string) => tag.length > 0);
    if (category && !tags.includes(category)) {
        tags.unshift(category);
    }
    const endDate =
        (market?.endDateIso ?? market?.endDate ?? market?.events?.[0]?.endDate ?? null) as
            | string
            | null;
    const openInterestRaw = market?.openInterest ?? market?.events?.[0]?.openInterest ?? null;
    const openInterest = parseOptionalNumber(openInterestRaw);
    const volumeRaw =
        market?.volumeNum ?? market?.volume24hr ?? market?.volume ?? market?.events?.[0]?.volume ?? null;
    const volume = parseOptionalNumber(volumeRaw);
    const question = (market?.question ?? market?.events?.[0]?.title ?? null) as string | null;
    return {
        slug,
        question,
        category,
        tags,
        endDate,
        openInterest,
        volume,
    };
};

const fetchMarketCategoryInfo = async (slug: string): Promise<MarketCategoryInfo | null> => {
    const cleaned = slug.trim();
    if (!cleaned) {
        return null;
    }
    const cached = marketCategoryCache.get(cleaned);
    if (cached) {
        return cached;
    }
    try {
        const markets = await fetchGammaMarketsBySlug(cleaned);
        if (!markets.length) {
            return null;
        }
        const info = extractMarketCategoryInfo(markets[0], cleaned);
        marketCategoryCache.set(cleaned, info);
        return info;
    } catch {
        return null;
    }
};

const inferBinanceSymbolFromSlug = (slug: string): string | null => {
    const normalized = slug.toLowerCase();
    if (normalized.includes('btc')) {
        return 'BTCUSDC';
    }
    if (normalized.includes('eth')) {
        return 'ETHUSDC';
    }
    if (normalized.includes('sol')) {
        return 'SOLUSDC';
    }
    if (normalized.includes('xrp')) {
        return 'XRPUSDC';
    }
    return null;
};

const fetchBinanceTickerPrice = async (symbol: string): Promise<number | null> => {
    if (!symbol || !BINANCE_SYMBOLS.has(symbol)) {
        return null;
    }
    try {
        const data = await fetchData(`${BINANCE_API}/ticker/price?symbol=${symbol}`);
        const record = data as Record<string, unknown>;
        return parseOptionalNumber(record?.price ?? record?.lastPrice);
    } catch {
        return null;
    }
};

const fetchBinanceOpenPrice = async (
    symbol: string,
    timestampSec: number
): Promise<number | null> => {
    if (!symbol || !BINANCE_SYMBOLS.has(symbol)) {
        return null;
    }
    const startMs = timestampSec * 1000;
    const endMs = startMs + 60 * 1000;
    try {
        const url = `${BINANCE_API}/klines?symbol=${symbol}&interval=1m&startTime=${startMs}&endTime=${endMs}&limit=1`;
        const data = await fetchData(url);
        if (Array.isArray(data) && data.length > 0 && Array.isArray(data[0])) {
            const open = data[0][1];
            return parseOptionalNumber(open);
        }
        return null;
    } catch {
        return null;
    }
};

const fetchGammaMarketsPage = async (limit: number, offset: number): Promise<GammaMarket[]> => {
    const url = `https://gamma-api.polymarket.com/markets?limit=${limit}&offset=${offset}`;
    const data = await fetchData(url);
    if (Array.isArray(data)) {
        return data;
    }
    if (data && typeof data === 'object') {
        const payload = data as { data?: GammaMarket[]; markets?: GammaMarket[] };
        if (Array.isArray(payload.data)) {
            return payload.data;
        }
        if (Array.isArray(payload.markets)) {
            return payload.markets;
        }
    }
    return [];
};

const fetchGammaMarketsBySeriesId = async (
    seriesId: string,
    limit: number,
    offset: number
): Promise<GammaMarket[]> => {
    const url = `https://gamma-api.polymarket.com/markets?series_id=${encodeURIComponent(
        seriesId
    )}&limit=${limit}&offset=${offset}`;
    const data = await fetchData(url);
    if (Array.isArray(data)) {
        return data;
    }
    if (data && typeof data === 'object') {
        const payload = data as { data?: GammaMarket[]; markets?: GammaMarket[] };
        if (Array.isArray(payload.data)) {
            return payload.data;
        }
        if (Array.isArray(payload.markets)) {
            return payload.markets;
        }
    }
    return [];
};

const getMarketTimestampSeconds = (market: GammaMarket): number | null => {
    const slug = market.slug ?? '';
    const slugTs = slug ? parseSlugTimestamp(slug) : null;
    if (slugTs !== null) {
        return slugTs;
    }
    return parseDateToSeconds(market.endDateIso ?? market.endDate ?? undefined);
};

const mapMarketSeriesEntry = (market: GammaMarket): MarketSeriesEntry => {
    const outcomes = Array.isArray(market.outcomes) ? market.outcomes : [];
    const clobTokenIds = Array.isArray(market.clobTokenIds) ? market.clobTokenIds : [];
    return {
        id: market.id ?? 0,
        question: market.question ?? 'Untitled market',
        slug: market.slug ?? undefined,
        conditionId: (market.conditionId ?? market.condition_id ?? undefined) as string | undefined,
        endDate: market.endDateIso ?? market.endDate ?? null,
        timestamp: getMarketTimestampSeconds(market),
        outcomes,
        clobTokenIds,
    };
};

const buildMarketTokenChoices = (market: GammaMarket): MarketTokenChoice[] => {
    const tokens = normalizeTokenIds(market.clobTokenIds);
    const outcomes = normalizeOutcomes(market.outcomes);
    return tokens.map((tokenId, index) => ({
        tokenId,
        outcome: outcomes[index] ?? undefined,
        outcomeIndex: index,
    }));
};

const pickTokenChoice = (
    choices: MarketTokenChoice[],
    outcome?: string,
    outcomeIndex?: number
): MarketTokenChoice | null => {
    if (typeof outcomeIndex === 'number' && Number.isFinite(outcomeIndex)) {
        return choices.find((choice) => choice.outcomeIndex === outcomeIndex) ?? null;
    }
    if (outcome) {
        const normalized = outcome.trim().toLowerCase();
        return (
            choices.find(
                (choice) => (choice.outcome ?? '').toLowerCase() === normalized
            ) ?? null
        );
    }
    if (choices.length === 1) {
        return choices[0];
    }
    return null;
};

const fetchRecentTrades = async (limit: number) => {
    const trades: Array<{
        id: string;
        userAddress: string;
        title?: string;
        slug?: string;
        eventSlug?: string;
        side?: string;
        usdcSize?: number;
        timestamp?: number;
        transactionHash?: string;
    }> = [];

    for (const address of ENV.USER_ADDRESSES) {
        const ActivityModel = getUserActivityModel(address);
        const docs = await ActivityModel.find(
            { type: 'TRADE' },
            { sort: { timestamp: -1 }, limit }
        );

        for (const doc of docs) {
            const title = doc.title ?? doc.slug ?? doc.asset ?? undefined;
            trades.push({
                id: doc._id?.toString() || `${address}-${doc.transactionHash || doc.timestamp}`,
                userAddress: address,
                title,
                slug: doc.slug ?? undefined,
                eventSlug: doc.eventSlug ?? undefined,
                side: doc.side ?? undefined,
                usdcSize: doc.usdcSize ?? undefined,
                timestamp: doc.timestamp ?? undefined,
                transactionHash: doc.transactionHash ?? undefined,
            });
        }
    }

    trades.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    return trades.slice(0, limit);
};

type TradeActivity = {
    id: string;
    userAddress: string;
    title?: string;
    slug?: string;
    eventSlug?: string;
    side?: string;
    outcome?: string;
    outcomeIndex?: number;
    asset?: string;
    market?: string;
    price?: number;
    size?: number;
    usdcSize?: number;
    timestamp?: number;
    transactionHash?: string;
};

type PositionHistoryEntry = {
    key: string;
    asset: string;
    market: string;
    slug?: string;
    outcome: string;
    side: 'buy' | 'sell';
    tradePrice: number;
    tradeSize: number;
    tradeUsdc: number;
    timestamp: number;
    positionSize: number;
    positionCostBasis: number;
    positionAvgPrice: number;
    realizedPnl: number;
    cumulativeRealizedPnl: number;
    status: 'OPEN' | 'CLOSED';
};

type PositionSummaryEntry = {
    key: string;
    asset: string;
    market: string;
    slug?: string;
    outcome: string;
    buySize: number;
    buyCost: number;
    avgEntry: number;
    sellSize: number;
    sellProceeds: number;
    avgExit: number;
    realizedPnl: number;
    unrealizedPnl: number | null;
    totalPnl: number;
    openSize: number;
    lastPrice: number | null;
    lastTradeAt: number | null;
    status: 'OPEN' | 'CLOSED';
};

type TraderProfileSummary = {
    totalTrades: number;
    wins: number;
    losses: number;
    winRate: number | null;
    realizedPnl: number;
    unrealizedPnl: number;
    totalPnl: number;
    openPositions: number;
    closedPositions: number;
    balance: number | null;
    positionsValue: number | null;
    portfolioValue: number | null;
};

type CategorySummaryEntry = {
    category: string;
    realizedPnl: number;
    unrealizedPnl: number;
    totalPnl: number;
    tradeCount: number;
    winCount: number;
    lossCount: number;
};

type DailyTradePnlEntry = {
    date: string;
    net: number;
    profit: number;
    loss: number;
    trades: number;
};

type ActivityRangeParams = {
    address: string;
    limit: number;
    slug?: string;
    offset?: number;
    startTs?: number | null;
    endTs?: number | null;
};

type ActivityRangeResult = {
    trades: TradeActivity[];
    nextOffset: number;
    exhausted: boolean;
};

type ActivityCursorResult = {
    trades: TradeActivity[];
    nextEnd: number | null;
    exhausted: boolean;
    oldest: number | null;
};

const mapActivityItem = (item: any, address: string): TradeActivity => ({
    id: item.id || `${item.transactionHash || item.timestamp}`,
    userAddress: address,
    title: item.title || item.slug || item.asset,
    slug: item.slug ?? undefined,
    eventSlug: item.eventSlug ?? undefined,
    side: item.side ?? undefined,
    outcome: item.outcome ?? undefined,
    outcomeIndex: item.outcomeIndex ?? undefined,
    asset: item.asset ?? undefined,
    market: item.market ?? undefined,
    price: item.price ?? undefined,
    size: item.size ?? undefined,
    usdcSize: item.usdcSize ?? undefined,
    timestamp: item.timestamp ?? undefined,
    transactionHash: item.transactionHash ?? undefined,
});

const getActivityTimestampSeconds = (item: any): number | null => {
    const raw = parseNumber(item?.timestamp);
    if (!raw) {
        return null;
    }
    return normalizeTimestampSeconds(raw);
};

const getOldestTimestampSeconds = (items: any[]): number | null => {
    let oldest: number | null = null;
    for (const item of items) {
        const timestamp = getActivityTimestampSeconds(item);
        if (timestamp === null) {
            continue;
        }
        if (oldest === null || timestamp < oldest) {
            oldest = timestamp;
        }
    }
    return oldest;
};

const getTradeTimestampSeconds = (trade: any): number | null => {
    const raw = parseNumber(trade?.timestamp ?? trade?.createdAt ?? trade?.time);
    if (!raw) {
        return null;
    }
    return normalizeTimestampSeconds(raw);
};

const mapMarketTradeEntry = (trade: any, market: string): MarketTradeEntry => ({
    id:
        trade?.id ||
        trade?.transactionHash ||
        trade?.txHash ||
        trade?.orderHash ||
        `${market}-${trade?.timestamp || trade?.createdAt || ''}-${trade?.owner || trade?.user || ''}`,
    market,
    owner: (trade?.owner || trade?.user || trade?.trader || trade?.address || '').toLowerCase(),
    side: trade?.side ?? trade?.action ?? undefined,
    outcome: trade?.outcome ?? undefined,
    outcomeIndex:
        typeof trade?.outcomeIndex === 'number' ? trade.outcomeIndex : undefined,
            price: parseOptionalNumber(trade?.price) ?? undefined,
            size: parseOptionalNumber(trade?.size) ?? undefined,
            usdcSize:
                parseOptionalNumber(trade?.usdcSize ?? trade?.usdc_size ?? trade?.value) ??
                undefined,
    timestamp: getTradeTimestampSeconds(trade) ?? undefined,
    transactionHash: trade?.transactionHash ?? trade?.txHash ?? trade?.hash ?? undefined,
});

const fetchActivityByUser = async (
    address: string,
    limit: number,
    slug?: string,
    offset = 0
): Promise<TradeActivity[]> => {
    const safeOffset = Number.isFinite(offset) ? Math.max(Math.floor(offset), 0) : 0;
    const offsetQuery = safeOffset > 0 ? `&offset=${safeOffset}` : '';
    const url = `https://data-api.polymarket.com/activity?user=${address}&type=TRADE&limit=${limit}${offsetQuery}`;
    const data = await fetchData(url);
    if (!Array.isArray(data)) {
        return [];
    }

    const slugFilter = slug?.trim().toLowerCase();
    const filtered = slugFilter
        ? data.filter((item: any) => {
              const haystack = `${item.slug || ''} ${item.eventSlug || ''} ${item.title || ''}`.toLowerCase();
              return haystack.includes(slugFilter);
          })
        : data;

    return filtered.map((item: any) => mapActivityItem(item, address));
};

const fetchActivityByUserWithRange = async ({
    address,
    limit,
    slug,
    offset = 0,
    startTs,
    endTs,
}: ActivityRangeParams): Promise<ActivityRangeResult> => {
    const startedAt = Date.now();
    const safeOffset = Number.isFinite(offset) ? Math.max(Math.floor(offset), 0) : 0;
    const maxEvents = Math.min(Math.max(limit, 1), ACTIVITY_RANGE_MAX_EVENTS);
    const slugFilter = slug?.trim().toLowerCase();
    const trades: TradeActivity[] = [];
    let currentOffset = safeOffset;
    let exhausted = false;
    let stopReason: string | null = null;
    let consecutiveEmpty = 0;
    const MAX_CONSECUTIVE_EMPTY = 25;

    if (ACTIVITY_RANGE_LOG) {
        Logger.info(
            `Activity range start: user=${address} limit=${maxEvents} offset=${safeOffset} startTs=${startTs ?? '-'} endTs=${endTs ?? '-'}`
        );
    }

    while (trades.length < maxEvents) {
        const remaining = maxEvents - trades.length;
        const batchLimit = Math.min(ACTIVITY_RANGE_BATCH_SIZE, remaining);
        const offsetQuery = currentOffset > 0 ? `&offset=${currentOffset}` : '';
        const url = `https://data-api.polymarket.com/activity?user=${address}&type=TRADE&limit=${batchLimit}${offsetQuery}`;
        const batchStart = Date.now();
        const data = await fetchData(url);
        if (!Array.isArray(data) || data.length === 0) {
            exhausted = true;
            stopReason = 'empty';
            break;
        }

        const filtered = data.filter((item: any) => {
            const timestamp = getActivityTimestampSeconds(item);
            if (!isTimestampInRange(timestamp, startTs, endTs)) {
                return false;
            }
            if (slugFilter) {
                const haystack = `${item.slug || ''} ${item.eventSlug || ''} ${item.title || ''}`.toLowerCase();
                return haystack.includes(slugFilter);
            }
            return true;
        });

        for (const item of filtered) {
            trades.push(mapActivityItem(item, address));
            if (trades.length >= maxEvents) {
                break;
            }
        }

        if (filtered.length === 0) {
            consecutiveEmpty += 1;
        } else {
            consecutiveEmpty = 0;
        }

        if (ACTIVITY_RANGE_LOG) {
            const oldest = getOldestTimestampSeconds(data);
            Logger.info(
                `Activity range batch: offset=${currentOffset} size=${data.length} filtered=${filtered.length} total=${trades.length} oldest=${oldest ?? '-'} ms=${Date.now() - batchStart}`
            );
        }

        currentOffset += data.length;

        if (data.length < batchLimit) {
            exhausted = true;
            stopReason = 'end';
            break;
        }

        if (consecutiveEmpty >= MAX_CONSECUTIVE_EMPTY) {
            exhausted = true;
            stopReason = 'empty_window';
            break;
        }

        if (startTs !== null && startTs !== undefined) {
            const oldest = getOldestTimestampSeconds(data);
            if (oldest !== null && oldest < startTs) {
                exhausted = true;
                stopReason = 'past_start';
                break;
            }
        }
    }

    if (ACTIVITY_RANGE_LOG) {
        Logger.info(
            `Activity range done: user=${address} total=${trades.length} nextOffset=${currentOffset} exhausted=${exhausted} reason=${stopReason ?? 'limit'} ms=${Date.now() - startedAt}`
        );
    }

    return { trades, nextOffset: currentOffset, exhausted };
};

const fetchActivityByUserWithCursor = async ({
    address,
    limit,
    slug,
    startTs,
    endTs,
}: ActivityRangeParams): Promise<ActivityCursorResult> => {
    const batchLimit = Math.min(Math.max(limit, 1), ACTIVITY_RANGE_BATCH_SIZE);
    const params = new URLSearchParams();
    params.set('user', address);
    params.set('type', 'TRADE');
    params.set('limit', String(batchLimit));
    if (endTs !== null && endTs !== undefined) {
        params.set('end', String(endTs));
    }
    if (startTs !== null && startTs !== undefined) {
        params.set('start', String(startTs));
    }

    const url = `https://data-api.polymarket.com/activity?${params.toString()}`;
    const data = await fetchData(url);
    if (!Array.isArray(data) || data.length === 0) {
        return { trades: [], nextEnd: null, exhausted: true, oldest: null };
    }

    const oldest = getOldestTimestampSeconds(data);
    const slugFilter = slug?.trim().toLowerCase();
    const filtered = data.filter((item: any) => {
        const timestamp = getActivityTimestampSeconds(item);
        if (!isTimestampInRange(timestamp, startTs, endTs)) {
            return false;
        }
        if (slugFilter) {
            const haystack = `${item.slug || ''} ${item.eventSlug || ''} ${item.title || ''}`.toLowerCase();
            return haystack.includes(slugFilter);
        }
        return true;
    });

    const trades = filtered.map((item: any) => mapActivityItem(item, address));
    const nextEnd = oldest !== null ? Math.max(oldest, 0) : null;
    let exhausted = data.length < batchLimit || oldest === null;
    if (startTs !== null && startTs !== undefined && oldest !== null && oldest <= startTs) {
        exhausted = true;
    }

    return { trades, nextEnd, exhausted, oldest };
};

const fetchActivityRawByUser = async (address: string, limit: number, offset = 0) => {
    const safeOffset = Number.isFinite(offset) ? Math.max(Math.floor(offset), 0) : 0;
    const offsetQuery = safeOffset > 0 ? `&offset=${safeOffset}` : '';
    const url = `https://data-api.polymarket.com/activity?user=${address}&limit=${limit}${offsetQuery}`;
    const data = await fetchData(url);
    return Array.isArray(data) ? data : [];
};

const fetchEarliestTradeTimestamp = async (address: string) => {
    let offset = 0;
    let scanned = 0;
    let earliest: number | null = null;
    let exhausted = false;
    let truncated = false;

    while (scanned < ACTIVITY_EARLIEST_MAX_EVENTS) {
        const remaining = ACTIVITY_EARLIEST_MAX_EVENTS - scanned;
        const limit = Math.min(ACTIVITY_RANGE_BATCH_SIZE, remaining);
        const offsetQuery = offset > 0 ? `&offset=${offset}` : '';
        const url = `https://data-api.polymarket.com/activity?user=${address}&type=TRADE&limit=${limit}${offsetQuery}`;
        const data = await fetchData(url);
        if (!Array.isArray(data) || data.length === 0) {
            exhausted = true;
            break;
        }

        const oldest = getOldestTimestampSeconds(data);
        if (oldest !== null && (earliest === null || oldest < earliest)) {
            earliest = oldest;
        }

        scanned += data.length;
        offset += data.length;

        if (data.length < limit) {
            exhausted = true;
            break;
        }
    }

    if (!exhausted && scanned >= ACTIVITY_EARLIEST_MAX_EVENTS) {
        truncated = true;
    }

    return { earliest, scanned, exhausted, truncated };
};

const fetchProxyTrades = async (limit: number) => {
    const url = `https://data-api.polymarket.com/activity?user=${ENV.PROXY_WALLET}&type=TRADE&limit=${limit}`;
    const data = await fetchData(url);
    if (!Array.isArray(data)) {
        return [];
    }

    return data.map((item: any) => ({
        id: item.id || `${item.transactionHash || item.timestamp}`,
        userAddress: ENV.PROXY_WALLET,
        title: item.title || item.slug || item.asset,
        slug: item.slug ?? undefined,
        eventSlug: item.eventSlug ?? undefined,
        side: item.side ?? undefined,
        usdcSize: item.usdcSize ?? undefined,
        timestamp: item.timestamp ?? undefined,
        transactionHash: item.transactionHash ?? undefined,
    }));
};

app.get('/api/status', (_req: Request, res: Response) => {
    res.json(getBotStatus());
});

app.post('/api/login', async (req: Request, res: Response) => {
    if (!LOGIN_HASH) {
        res.status(500).json({ error: 'HASH not configured on server' });
        return;
    }

    const captcha = await verifyRecaptcha(getRecaptchaToken(req), 'login', req.ip);
    if (!captcha.ok) {
        res.status(403).json({ error: captcha.error || 'Recaptcha failed' });
        return;
    }

    const password = typeof req.body?.password === 'string' ? req.body.password : '';
    if (!password) {
        res.status(400).json({ error: 'Password is required' });
        return;
    }

    const match = await bcrypt.compare(password, LOGIN_HASH);
    if (!match) {
        res.status(401).json({ error: 'Invalid password' });
        return;
    }

    const token = randomUUID();
    authTokens.set(token, {
        expiresAt: Date.now() + TOKEN_TTL_MS,
        legacy: true,
        role: 'admin',
        email: 'legacy',
    });
    res.json({ token, expiresInMs: TOKEN_TTL_MS, legacy: true, role: 'admin' });
});

app.post('/api/auth/register', async (req: Request, res: Response) => {
    const emailInput = typeof req.body?.email === 'string' ? req.body.email : '';
    const password = typeof req.body?.password === 'string' ? req.body.password : '';
    const email = normalizeEmail(emailInput);

    if (!isValidEmail(email)) {
        res.status(400).json({ error: 'Valid email is required' });
        return;
    }
    if (!isValidPassword(password)) {
        res.status(400).json({ error: 'Password must be at least 8 characters' });
        return;
    }

    const captcha = await verifyRecaptcha(getRecaptchaToken(req), 'signup', req.ip);
    if (!captcha.ok) {
        res.status(403).json({ error: captcha.error || 'Recaptcha failed' });
        return;
    }

    try {
        await ensureDbConnection();
        const existing = await UserModel.findOne({ email });
        if (existing) {
            res.status(409).json({ error: 'Email already registered' });
            return;
        }

        const totalUsers = await UserModel.countDocuments();
        const role: UserRole = totalUsers === 0 ? 'admin' : 'user';
        const passwordHash = await bcrypt.hash(password, 10);
        const now = new Date().toISOString();
        const user = await UserModel.create({
            email,
            passwordHash,
            role,
            walletAddress: '',
            lastLoginAt: now,
        });

        await logUserActivity(user._id, 'register', { role });

        const token = randomUUID();
        authTokens.set(token, {
            expiresAt: Date.now() + TOKEN_TTL_MS,
            userId: user._id,
            role: user.role,
            email: user.email,
        });
        res.json({ token, expiresInMs: TOKEN_TTL_MS, user: sanitizeUser(user) });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.post('/api/auth/login', async (req: Request, res: Response) => {
    const emailInput = typeof req.body?.email === 'string' ? req.body.email : '';
    const password = typeof req.body?.password === 'string' ? req.body.password : '';
    const email = normalizeEmail(emailInput);

    if (!isValidEmail(email) || !password) {
        res.status(400).json({ error: 'Email and password are required' });
        return;
    }

    const captcha = await verifyRecaptcha(getRecaptchaToken(req), 'login', req.ip);
    if (!captcha.ok) {
        res.status(403).json({ error: captcha.error || 'Recaptcha failed' });
        return;
    }

    try {
        await ensureDbConnection();
        const user = await UserModel.findOne({ email });
        if (!user) {
            res.status(401).json({ error: 'Invalid credentials' });
            return;
        }

        const match = await bcrypt.compare(password, user.passwordHash);
        if (!match) {
            res.status(401).json({ error: 'Invalid credentials' });
            return;
        }

        const now = new Date().toISOString();
        await UserModel.findByIdAndUpdate(user._id, { lastLoginAt: now }, { new: true });
        user.lastLoginAt = now;

        await logUserActivity(user._id, 'login', {});

        const token = randomUUID();
        authTokens.set(token, {
            expiresAt: Date.now() + TOKEN_TTL_MS,
            userId: user._id,
            role: user.role,
            email: user.email,
        });
        res.json({ token, expiresInMs: TOKEN_TTL_MS, user: sanitizeUser(user) });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/auth/me', requireUser, async (req: Request, res: Response) => {
    const session = (req as AuthRequest).auth;
    try {
        await ensureDbConnection();
        const user = await UserModel.findById(session?.userId);
        if (!user) {
            res.status(404).json({ error: 'User not found' });
            return;
        }
        res.json({ user: sanitizeUser(user) });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.patch('/api/auth/wallet', requireUser, async (req: Request, res: Response) => {
    const session = (req as AuthRequest).auth;
    const walletInput =
        typeof req.body?.walletAddress === 'string' ? req.body.walletAddress.trim() : '';
    const walletAddress = walletInput.toLowerCase();

    if (!walletAddress) {
        res.status(400).json({ error: 'Wallet address is required' });
        return;
    }
    if (!isValidEthereumAddress(walletAddress)) {
        res.status(400).json({ error: 'Invalid wallet address' });
        return;
    }

    try {
        await ensureDbConnection();
        const user = await UserModel.findByIdAndUpdate(session?.userId || '', { walletAddress }, {
            new: true,
        });

        if (!user) {
            res.status(404).json({ error: 'User not found' });
            return;
        }

        const updatedId = (user as any)?._id?.toString?.() ?? '';
        if (updatedId) {
            await logUserActivity(updatedId, 'wallet_update', { walletAddress });
        }

        res.json({ user: sanitizeUser(user) });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

const clearAuthToken = (req: Request) => {
    const token = getAuthToken(req);
    if (token) {
        authTokens.delete(token);
    }
};

app.post('/api/logout', requireAuth, async (req: Request, res: Response) => {
    const session = (req as AuthRequest).auth;
    if (session?.userId) {
        await logUserActivity(session.userId, 'logout', {});
    }
    clearAuthToken(req);
    res.json({ ok: true });
});

app.post('/api/auth/logout', requireAuth, async (req: Request, res: Response) => {
    const session = (req as AuthRequest).auth;
    if (session?.userId) {
        await logUserActivity(session.userId, 'logout', {});
    }
    clearAuthToken(req);
    res.json({ ok: true });
});

app.get('/api/auth/status', (req: Request, res: Response) => {
    const session = getAuthSession(getAuthToken(req));
    if (!session) {
        res.status(401).json({ authenticated: false });
        return;
    }
    if (session.legacy) {
        res.json({ authenticated: true, legacy: true, role: 'admin' });
        return;
    }
    res.json({
        authenticated: true,
        user: {
            id: session.userId,
            email: session.email,
            role: session.role,
        },
    });
});

app.post('/api/start', requireAuth, async (_req: Request, res: Response) => {
    if (ANALYSIS_ONLY) {
        res.status(403).json({ error: 'Trading is disabled in analysis mode' });
        return;
    }
    try {
        const status = await startBot({ showWelcome: false, runHealthCheck: false });
        res.json(status);
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.post('/api/stop', requireAuth, async (_req: Request, res: Response) => {
    if (ANALYSIS_ONLY) {
        res.status(403).json({ error: 'Trading is disabled in analysis mode' });
        return;
    }
    try {
        const status = await stopBot({ closeDb: false });
        res.json(status);
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.get('/api/health', async (_req: Request, res: Response) => {
    try {
        const result = await performHealthCheck();
        res.json(result);
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.get('/api/logs', requireAuth, (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || '200', 10), 1000);
    const lines = readLogLines(limit);
    res.json({ lines });
});

app.get('/api/admin/users', requireAdmin, async (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || '50', 10), 200);

    try {
        await ensureDbConnection();
        const users = await UserModel.find({}, { sort: { createdAt: -1 }, limit });
        res.json({ users: users.map((user) => sanitizeUser(user)) });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.patch('/api/admin/users/:id', requireAdmin, async (req: Request, res: Response) => {
    const userId = req.params.id;
    if (!isValidId(userId)) {
        res.status(400).json({ error: 'Invalid user id' });
        return;
    }

    const roleInput = typeof req.body?.role === 'string' ? req.body.role : '';
    const walletInput =
        typeof req.body?.walletAddress === 'string' ? req.body.walletAddress.trim() : '';
    const updates: Record<string, unknown> = {};

    if (roleInput) {
        if (roleInput !== 'admin' && roleInput !== 'user') {
            res.status(400).json({ error: 'Invalid role' });
            return;
        }
        updates.role = roleInput;
    }

    if (walletInput) {
        const walletAddress = walletInput.toLowerCase();
        if (!isValidEthereumAddress(walletAddress)) {
            res.status(400).json({ error: 'Invalid wallet address' });
            return;
        }
        updates.walletAddress = walletAddress;
    }

    if (Object.keys(updates).length === 0) {
        res.status(400).json({ error: 'No updates provided' });
        return;
    }

    try {
        await ensureDbConnection();
        const updated = await UserModel.findByIdAndUpdate(userId, updates, { new: true });

        if (!updated) {
            res.status(404).json({ error: 'User not found' });
            return;
        }

        const session = (req as AuthRequest).auth;
        if (session?.userId) {
            await logUserActivity(session.userId, 'admin_update_user', {
                targetUserId: userId,
                updates,
            });
        }

        res.json({ user: sanitizeUser(updated) });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/admin/activity', requireAdmin, async (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || '50', 10), 200);
    const offsetRaw = parseInt((req.query.offset as string) || '0', 10);
    const offset = Number.isNaN(offsetRaw) ? 0 : Math.max(offsetRaw, 0);
    const userId = typeof req.query.userId === 'string' ? req.query.userId : '';

    const query: Record<string, unknown> = {};
    if (userId) {
        if (!isValidId(userId)) {
            res.status(400).json({ error: 'Invalid user id' });
            return;
        }
        query.userId = userId;
    }

    try {
        await ensureDbConnection();
        const logs = await UserActivityLog.find(query, {
            sort: { createdAt: -1 },
            skip: offset,
            limit,
        });

        const userIds = logs
            .map((log) => log.userId?.toString())
            .filter((id): id is string => Boolean(id));
        const users = userIds.length
            ? await UserModel.find({ _id: { $in: userIds } })
            : [];
        const userMap = new Map<string, { id: string; email: string; role: UserRole }>();
        for (const user of users) {
            const userId = (user as any)?._id?.toString?.() ?? '';
            if (!userId) {
                continue;
            }
            userMap.set(userId, { id: userId, email: user.email, role: user.role });
        }

        res.json({
            logs: logs.map((log) => ({
                id: log._id?.toString() ?? '',
                action: log.action,
                createdAt: log.createdAt ? new Date(log.createdAt).toISOString() : null,
                user: userMap.get(log.userId?.toString()) ?? null,
                metadata: log.metadata ?? {},
            })),
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/trades', async (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || '20', 10), 200);
    const scope = (req.query.scope as string) || 'traders';

    try {
        const trades = scope === 'proxy' ? await fetchProxyTrades(limit) : await fetchRecentTrades(limit);
        res.json({ trades });
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.get('/api/activity', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user : '';
    const slug = typeof req.query.slug === 'string' ? req.query.slug : '';
    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);
    const hasRange = startTs !== null || endTs !== null;
    const cursorMode =
        req.query.cursor === '1' ||
        req.query.cursor === 'true' ||
        req.query.mode === 'cursor';
    const limitCap = hasRange ? ACTIVITY_RANGE_MAX_EVENTS : ACTIVITY_REQUEST_LIMIT;
    const limitRaw = parseInt((req.query.limit as string) || '20', 10);
    const limit = Number.isNaN(limitRaw)
        ? 20
        : Math.min(Math.max(limitRaw, 1), limitCap);
    const offsetRaw = parseInt((req.query.offset as string) || '0', 10);
    const offset = Number.isNaN(offsetRaw) ? 0 : Math.max(offsetRaw, 0);

    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }
    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    try {
        if (cursorMode) {
            const result = await fetchActivityByUserWithCursor({
                address: user,
                limit,
                slug,
                startTs,
                endTs,
            });
            res.json({
                trades: result.trades,
                nextEnd: result.nextEnd,
                oldest: result.oldest,
                exhausted: result.exhausted,
            });
            return;
        }
        if (hasRange) {
            const result = await fetchActivityByUserWithRange({
                address: user,
                limit,
                slug,
                offset,
                startTs,
                endTs,
            });
            res.json({
                trades: result.trades,
                nextOffset: result.nextOffset,
                exhausted: result.exhausted,
            });
            return;
        }
        const trades = await fetchActivityByUser(user, limit, slug, offset);
        res.json({ trades });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/activity/earliest', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user : '';
    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }

    try {
        const result = await fetchEarliestTradeTimestamp(user);
        if (!result.earliest) {
            res.status(404).json({ error: 'No activity found' });
            return;
        }
        res.json({
            earliestTimestamp: result.earliest,
            scanned: result.scanned,
            exhausted: result.exhausted,
            truncated: result.truncated,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/positions', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user : '';
    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }

    const limitRaw = parseInt((req.query.limit as string) || '50', 10);
    const limit = Number.isNaN(limitRaw) ? 50 : Math.min(Math.max(limitRaw, 1), 200);
    const offsetRaw = parseInt((req.query.offset as string) || '0', 10);
    const offset = Number.isNaN(offsetRaw) ? 0 : Math.max(offsetRaw, 0);
    const sortBy = typeof req.query.sortBy === 'string' ? req.query.sortBy : 'CURRENT';
    const sortDirection =
        typeof req.query.sortDirection === 'string' ? req.query.sortDirection : 'DESC';
    const sizeThreshold =
        typeof req.query.sizeThreshold === 'string' ? req.query.sizeThreshold : '.1';

    try {
        const url = `https://data-api.polymarket.com/positions?user=${user}&sortBy=${encodeURIComponent(
            sortBy
        )}&sortDirection=${encodeURIComponent(
            sortDirection
        )}&sizeThreshold=${encodeURIComponent(
            sizeThreshold
        )}&limit=${limit}&offset=${offset}`;
        const data = await fetchData(url);
        if (data && typeof data === 'object' && !Array.isArray(data)) {
            const payload = data as { value?: unknown[]; Count?: number };
            res.json({ positions: payload.value ?? [], count: payload.Count ?? null });
            return;
        }
        res.json({ positions: Array.isArray(data) ? data : [], count: null });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/closed-positions', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user : '';
    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }

    const limitRaw = parseInt((req.query.limit as string) || '25', 10);
    const limit = Number.isNaN(limitRaw) ? 25 : Math.min(Math.max(limitRaw, 1), 200);
    const offsetRaw = parseInt((req.query.offset as string) || '0', 10);
    const offset = Number.isNaN(offsetRaw) ? 0 : Math.max(offsetRaw, 0);
    const sortBy =
        typeof req.query.sortBy === 'string' ? req.query.sortBy : 'realizedpnl';
    const sortDirection =
        typeof req.query.sortDirection === 'string' ? req.query.sortDirection : 'DESC';

    try {
        const url = `https://data-api.polymarket.com/closed-positions?user=${user}&sortBy=${encodeURIComponent(
            sortBy
        )}&sortDirection=${encodeURIComponent(
            sortDirection
        )}&limit=${limit}&offset=${offset}`;
        const data = await fetchData(url);
        if (data && typeof data === 'object' && !Array.isArray(data)) {
            const payload = data as { value?: unknown[]; Count?: number };
            res.json({ positions: payload.value ?? [], count: payload.Count ?? null });
            return;
        }
        res.json({ positions: Array.isArray(data) ? data : [], count: null });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/trader/profile', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user.trim() : '';
    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);
    const hasRange = startTs !== null || endTs !== null;
    const limitRaw = parseInt((req.query.limit as string) || '5000', 10);
    const limit = Number.isNaN(limitRaw)
        ? 5000
        : Math.min(Math.max(limitRaw, 1), ACTIVITY_RANGE_MAX_EVENTS);

    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }
    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    try {
        const activityResult = hasRange
            ? await fetchActivityByUserWithRange({
                  address: user,
                  limit,
                  startTs,
                  endTs,
              })
            : { trades: await fetchActivityByUser(user, limit), nextOffset: limit, exhausted: false };

        const trades = activityResult.trades;
        const positions = computePositionSummary(trades);
        const positionHistory = computePositionHistory(trades);
        const dailyPnl = computeTradeDailyPnl(trades);
        const walletSummary = await fetchWalletSummaryData(user);
        const summary = buildTraderProfileSummary(
            positions,
            walletSummary.balance,
            walletSummary.positionsValue,
            walletSummary.portfolioValue,
            trades.length
        );

        const slugs = Array.from(
            new Set(
                trades
                    .map((trade) => trade.slug || trade.eventSlug || '')
                    .filter((slug) => slug.length > 0)
            )
        );
        const categoryMap = new Map<string, MarketCategoryInfo | null>();
        for (const slug of slugs) {
            const info = await fetchMarketCategoryInfo(slug);
            categoryMap.set(slug, info);
        }

        const tradesWithCategory = trades.map((trade) => {
            const slug = trade.slug || trade.eventSlug || '';
            const info = slug ? categoryMap.get(slug) ?? null : null;
            return {
                ...trade,
                category: info?.category ?? null,
                tags: info?.tags ?? [],
            };
        });
        const categories = buildCategorySummary(positions, categoryMap);

        res.json({
            summary,
            trades: tradesWithCategory,
            positions,
            positionHistory,
            dailyPnl,
            categories,
            range: {
                startTs,
                endTs,
                limit,
            },
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/traders/scan', async (req: Request, res: Response) => {
    const limitRaw = parseInt((req.query.limit as string) || '20', 10);
    const limit = Number.isNaN(limitRaw) ? 20 : Math.min(Math.max(limitRaw, 1), 200);
    const marketLimitRaw = parseInt((req.query.marketLimit as string) || '25', 10);
    const marketLimit = Number.isNaN(marketLimitRaw)
        ? 25
        : Math.min(Math.max(marketLimitRaw, 1), 200);
    const tradeLimitRaw = parseInt((req.query.tradeLimit as string) || '200', 10);
    const tradeLimit = Number.isNaN(tradeLimitRaw)
        ? 200
        : Math.min(Math.max(tradeLimitRaw, 1), 500);
    const sortMode = (req.query.sort as string) || 'volume';
    const status = ((req.query.status as string) || 'active').toLowerCase();
    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);

    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    try {
        const markets = await fetchGammaMarketsPage(marketLimit, 0);
        const filtered = markets.filter((market: any) => {
            if (status === 'active') {
                return market.active === true && market.closed !== true;
            }
            if (status === 'closed') {
                return market.closed === true;
            }
            return true;
        });
        const sorted = filtered.sort(
            (a: any, b: any) =>
                parseNumber(b.volume24hr || b.volume || b.volumeNum) -
                parseNumber(a.volume24hr || a.volume || a.volumeNum)
        );

        const traderMap = new Map<
            string,
            { address: string; tradeCount: number; volume: number; lastTradeAt: number | null }
        >();

        for (const market of sorted.slice(0, marketLimit)) {
            const conditionId = market.conditionId || market.condition_id;
            if (!conditionId) {
                continue;
            }
            const url = `https://data-api.polymarket.com/trades?market=${conditionId}&limit=${tradeLimit}`;
            const trades = await fetchData(url);
            if (!Array.isArray(trades)) {
                continue;
            }
            trades.forEach((trade: any) => {
                const owner =
                    (trade.owner || trade.user || trade.trader || trade.address || '').toLowerCase();
                if (!owner) {
                    return;
                }
                const timestamp = normalizeTimestampSeconds(
                    parseNumber(trade.timestamp || trade.createdAt || trade.time)
                );
                if (!isTimestampInRange(timestamp, startTs, endTs)) {
                    return;
                }
                const size = parseNumber(trade.size);
                const price = parseNumber(trade.price);
                const usdc = parseNumber(trade.usdcSize || trade.usdc_size || trade.value);
                const volume = usdc || (size > 0 && price > 0 ? size * price : 0);

                const entry = traderMap.get(owner) || {
                    address: owner,
                    tradeCount: 0,
                    volume: 0,
                    lastTradeAt: null,
                };
                entry.tradeCount += 1;
                entry.volume += volume;
                if (!entry.lastTradeAt || timestamp > entry.lastTradeAt) {
                    entry.lastTradeAt = timestamp || entry.lastTradeAt;
                }
                traderMap.set(owner, entry);
            });
        }

        const traders = Array.from(traderMap.values()).sort((a, b) => {
            if (sortMode === 'trades') {
                return b.tradeCount - a.tradeCount;
            }
            return b.volume - a.volume;
        });

        res.json({
            traders: traders.slice(0, limit),
            scannedMarkets: sorted.length,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/wallet-summary', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user : '';

    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }

    try {
        const positions = await fetchData(
            `https://data-api.polymarket.com/positions?user=${user}`
        );
        const list = Array.isArray(positions) ? positions : [];
        const balance = await getMyBalance(user);

        let positionsValue = 0;
        let initialValue = 0;
        let cashPnl = 0;
        let weightedPnl = 0;
        let totalProfit = 0;
        let totalLoss = 0;

        list.forEach((pos: any) => {
            const value = parseNumber(pos.currentValue);
            const initial = parseNumber(pos.initialValue);
            const pnlCash = parseNumber(pos.cashPnl);
            const pnlPercent = parseNumber(pos.percentPnl);
            positionsValue += value;
            initialValue += initial;
            cashPnl += pnlCash;
            weightedPnl += value * pnlPercent;
            if (pnlCash >= 0) {
                totalProfit += pnlCash;
            } else {
                totalLoss += Math.abs(pnlCash);
            }
        });

        const pnlPercent = positionsValue > 0 ? weightedPnl / positionsValue : 0;
        const portfolioValue = positionsValue + balance;

        res.json({
            summary: {
                wallet: user,
                balance,
                positionsValue,
                portfolioValue,
                totalInitialValue: initialValue,
                cashPnl,
                totalProfit,
                totalLoss,
                pnlPercent,
                positionsCount: list.length,
            },
        });
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.get('/api/wallet-lifetime', async (req: Request, res: Response) => {
    const user = typeof req.query.user === 'string' ? req.query.user : '';

    if (!isValidEthereumAddress(user)) {
        res.status(400).json({ error: 'Invalid user address' });
        return;
    }

    const cached = lifetimeCache.get(user);
    if (cached && cached.expiresAt > Date.now()) {
        res.json({ summary: cached.data });
        return;
    }

    try {
        let offset = 0;
        let allTrades: any[] = [];
        let truncated = false;

        while (true) {
            const batch = await fetchActivityRawByUser(user, LIFETIME_BATCH_SIZE, offset);
            if (batch.length === 0) {
                break;
            }
            allTrades = allTrades.concat(batch);
            offset += batch.length;
            if (batch.length < LIFETIME_BATCH_SIZE) {
                break;
            }
            if (allTrades.length >= LIFETIME_MAX_EVENTS) {
                truncated = true;
                break;
            }
        }

        const summary = computeLifetimeAnalytics(allTrades);
        const eventsCount = allTrades.length;
        const tradesCount = allTrades.filter(isTradeActivity).length;
        const payload = {
            totalProfit: summary.totalProfit,
            totalLoss: summary.totalLoss,
            netPnl: summary.netPnl,
            tradesCount,
            eventsCount,
            truncated,
            firstTradeAt: summary.firstTradeAt,
            lastTradeAt: summary.lastTradeAt,
            daily: summary.daily,
            winRate: summary.winRate,
            profitFactor: summary.profitFactor,
            avgWinDay: summary.avgWinDay,
            avgLossDay: summary.avgLossDay,
            maxDrawdown: summary.maxDrawdown,
            sharpeRatio: summary.sharpeRatio,
            lastDayNet: summary.lastDayNet,
            activeDays: summary.activeDays,
        };

        lifetimeCache.set(user, {
            expiresAt: Date.now() + LIFETIME_CACHE_TTL_MS,
            data: payload,
        });

        res.json({ summary: payload });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/price-history', async (req: Request, res: Response) => {
    const tokenIdParam = typeof req.query.tokenId === 'string' ? req.query.tokenId.trim() : '';
    const slugParam = typeof req.query.slug === 'string' ? req.query.slug.trim() : '';
    const marketParam = typeof req.query.market === 'string' ? req.query.market.trim() : '';
    let tokenId = tokenIdParam;
    let slug = slugParam;

    if (!tokenId && !slug && marketParam) {
        if (/^\d+$/.test(marketParam)) {
            tokenId = marketParam;
        } else {
            slug = marketParam;
        }
    }

    if (!tokenId && !slug) {
        res.status(400).json({ error: 'tokenId, slug, or market is required' });
        return;
    }

    const outcomeParam = typeof req.query.outcome === 'string' ? req.query.outcome.trim() : '';
    const outcomeIndexRaw = req.query.outcomeIndex ?? req.query.outcome_index;
    const outcomeIndexParsed =
        typeof outcomeIndexRaw === 'number'
            ? outcomeIndexRaw
            : typeof outcomeIndexRaw === 'string'
              ? parseInt(outcomeIndexRaw, 10)
              : null;
    const outcomeIndex =
        outcomeIndexParsed !== null && Number.isFinite(outcomeIndexParsed)
            ? Math.floor(outcomeIndexParsed)
            : null;

    const interval = typeof req.query.interval === 'string' ? req.query.interval.trim() : '';
    const fidelityRaw = req.query.fidelity;
    const fidelityParsed =
        typeof fidelityRaw === 'number'
            ? fidelityRaw
            : typeof fidelityRaw === 'string'
              ? parseInt(fidelityRaw, 10)
              : null;
    const fidelity =
        fidelityParsed !== null && Number.isFinite(fidelityParsed) ? Math.floor(fidelityParsed) : null;

    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);

    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    let selectedOutcome: string | undefined;
    let selectedOutcomeIndex: number | undefined;

    try {
        if (!tokenId && slug) {
            const markets = await fetchGammaMarketsBySlug(slug);
            if (!markets.length) {
                res.status(404).json({ error: 'Market not found for slug' });
                return;
            }
            const choicesMap = new Map<string, MarketTokenChoice>();
            for (const market of markets) {
                for (const choice of buildMarketTokenChoices(market)) {
                    if (!choicesMap.has(choice.tokenId)) {
                        choicesMap.set(choice.tokenId, choice);
                    }
                }
            }
            const choices = Array.from(choicesMap.values());
            const chosen = pickTokenChoice(
                choices,
                outcomeParam || undefined,
                outcomeIndex ?? undefined
            );
            if (!chosen) {
                res.status(400).json({
                    error: 'Multiple outcomes found; specify tokenId, outcome, or outcomeIndex',
                    options: choices,
                });
                return;
            }
            tokenId = chosen.tokenId;
            selectedOutcome = chosen.outcome;
            selectedOutcomeIndex = chosen.outcomeIndex;
        }

        if (!tokenId) {
            res.status(400).json({ error: 'tokenId could not be resolved' });
            return;
        }

        const params = new URLSearchParams();
        params.set('market', tokenId);
        if (interval) {
            params.set('interval', interval);
        } else {
            if (startTs !== null) {
                params.set('startTs', String(startTs));
            }
            if (endTs !== null) {
                params.set('endTs', String(endTs));
            }
        }
        if (fidelity !== null) {
            params.set('fidelity', String(fidelity));
        }

        const url = `https://clob.polymarket.com/prices-history?${params.toString()}`;
        const history = await fetchData(url);
        const eventPrices = slug ? await fetchEventPricesForSlug(slug) : null;
        const liquidityClob = await fetchOrderBookLiquidity(tokenId);
        res.json({
            tokenId,
            slug: slug || undefined,
            outcome: selectedOutcome ?? (outcomeParam || undefined),
            outcomeIndex:
                selectedOutcomeIndex ??
                (outcomeIndex !== null && outcomeIndex !== undefined ? outcomeIndex : undefined),
            history,
            priceToBeat: eventPrices?.priceToBeat ?? null,
            currentPrice: eventPrices?.currentPrice ?? null,
            liquidityClob,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/clob/book', async (req: Request, res: Response) => {
    const tokenId = typeof req.query.tokenId === 'string' ? req.query.tokenId.trim() : '';
    if (!tokenId) {
        res.status(400).json({ error: 'tokenId is required' });
        return;
    }
    const base = ENV.CLOB_HTTP_URL?.replace(/\/$/, '');
    if (!base) {
        res.status(500).json({ error: 'CLOB_HTTP_URL is not configured' });
        return;
    }

    try {
        const url = `${base}/book?token_id=${encodeURIComponent(tokenId)}`;
        const book = await fetchData(url);
        const snapshot = extractBookSnapshot(book);
        res.json({
            tokenId,
            ...snapshot,
        });
    } catch (error: any) {
        const status = error?.response?.status;
        if (status === 404) {
            res.status(404).json({ error: 'Order book not found' });
            return;
        }
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/clob/last-trade', async (req: Request, res: Response) => {
    const tokenId = typeof req.query.tokenId === 'string' ? req.query.tokenId.trim() : '';
    if (!tokenId) {
        res.status(400).json({ error: 'tokenId is required' });
        return;
    }
    const base = ENV.CLOB_HTTP_URL?.replace(/\/$/, '');
    if (!base) {
        res.status(500).json({ error: 'CLOB_HTTP_URL is not configured' });
        return;
    }

    try {
        const url = `${base}/last-trade-price?token_id=${encodeURIComponent(tokenId)}`;
        const data = await fetchData(url);
        const record = data as Record<string, unknown>;
        res.json({
            tokenId,
            price: parseOptionalNumber(record?.price ?? record?.last_price ?? record?.lastPrice),
            side: typeof record?.side === 'string' ? record.side : undefined,
        });
    } catch (error: any) {
        const status = error?.response?.status;
        if (status === 404) {
            res.status(404).json({ error: 'Last trade not found' });
            return;
        }
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/token-ids', async (req: Request, res: Response) => {
    const slug = typeof req.query.slug === 'string' ? req.query.slug.trim() : '';
    if (!slug) {
        res.status(400).json({ error: 'slug is required' });
        return;
    }

    try {
        const markets = await fetchGammaMarketsBySlug(slug);
        if (!markets.length) {
            res.status(404).json({ error: 'Market not found for slug' });
            return;
        }
        const choicesMap = new Map<string, MarketTokenChoice>();
        for (const market of markets) {
            for (const choice of buildMarketTokenChoices(market)) {
                if (!choicesMap.has(choice.tokenId)) {
                    choicesMap.set(choice.tokenId, choice);
                }
            }
        }
        const options = Array.from(choicesMap.values());
        res.json({
            slug,
            question: markets[0]?.question ?? undefined,
            options,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/price-snapshot', async (req: Request, res: Response) => {
    const slug = typeof req.query.slug === 'string' ? req.query.slug.trim() : '';
    if (!slug) {
        res.status(400).json({ error: 'slug is required' });
        return;
    }

    try {
        const snapshot = await fetchEventPricesForSlug(slug);
        let priceToBeat = snapshot.priceToBeat ?? null;
        let currentPrice = snapshot.currentPrice ?? null;

        if (priceToBeat === null || currentPrice === null) {
            const symbol = inferBinanceSymbolFromSlug(slug);
            if (symbol) {
                if (priceToBeat === null) {
                    const ts = parseSlugTimestamp(slug);
                    if (ts) {
                        priceToBeat = await fetchBinanceOpenPrice(symbol, ts);
                    }
                }
                if (currentPrice === null) {
                    currentPrice = await fetchBinanceTickerPrice(symbol);
                }
            }
        }
        res.json({
            slug,
            priceToBeat,
            currentPrice,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/series-id', async (req: Request, res: Response) => {
    const slug = typeof req.query.slug === 'string' ? req.query.slug.trim() : '';
    if (!slug) {
        res.status(400).json({ error: 'slug is required' });
        return;
    }

    try {
        const markets = await fetchGammaMarketsBySlug(slug);
        if (!markets.length) {
            res.status(404).json({ error: 'Market not found for slug' });
            return;
        }
        const first = markets[0];
        const seriesId = first.series_id ?? first.seriesId ?? null;
        if (!seriesId) {
            res.status(404).json({ error: 'series_id not found for slug' });
            return;
        }
        res.json({
            slug,
            seriesId: String(seriesId),
            question: first.question ?? undefined,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/resolve', async (req: Request, res: Response) => {
    const prefixInput = typeof req.query.prefix === 'string' ? req.query.prefix.trim() : '';
    const prefixRaw = prefixInput.toLowerCase();
    const prefix = stripSlugTimestamp(prefixRaw);
    if (!prefix) {
        res.status(400).json({ error: 'prefix is required' });
        return;
    }

    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);

    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    const limitRaw = parseInt((req.query.limit as string) || '200', 10);
    const limit = Number.isNaN(limitRaw) ? 200 : Math.min(Math.max(limitRaw, 1), 2000);

    try {
        const results: Array<{
            slug: string;
            tokenId: string;
            outcome?: string;
            outcomeIndex?: number;
            endDate?: string;
            timestamp?: number;
        }> = [];
        const seen = new Set<string>();
        const intervalSeconds = parsePrefixIntervalSeconds(prefix);
        if (intervalSeconds && startTs !== null && endTs !== null) {
            const alignedStart = alignTimestampToInterval(startTs, intervalSeconds, 'ceil');
            const alignedEnd = alignTimestampToInterval(endTs, intervalSeconds, 'floor');
            if (alignedStart <= alignedEnd) {
                const totalSteps =
                    Math.floor((alignedEnd - alignedStart) / intervalSeconds) + 1;
                let scanned = 0;
                let truncated = false;

                for (let index = 0; index < totalSteps; index += 1) {
                    if (results.length >= limit) {
                        truncated = true;
                        break;
                    }
                    const ts = alignedStart + index * intervalSeconds;
                    scanned += 1;
                    const slug = `${prefix}-${ts}`;
                    const markets = await fetchGammaMarketsBySlug(slug);
                    if (!markets.length) {
                        continue;
                    }
                    for (const market of markets) {
                        const marketTs = getMarketTimestampSeconds(market);
                        if (!isTimestampInRange(marketTs, startTs, endTs)) {
                            continue;
                        }
                        const endDate = market.endDateIso ?? market.endDate ?? undefined;
                        for (const choice of buildMarketTokenChoices(market)) {
                            const key = `${slug}-${choice.tokenId}-${choice.outcomeIndex ?? ''}`;
                            if (seen.has(key)) {
                                continue;
                            }
                            seen.add(key);
                            results.push({
                                slug,
                                tokenId: choice.tokenId,
                                outcome: choice.outcome,
                                outcomeIndex: choice.outcomeIndex,
                                endDate,
                                timestamp: marketTs ?? undefined,
                            });
                            if (results.length >= limit) {
                                truncated = true;
                                break;
                            }
                        }
                        if (results.length >= limit) {
                            break;
                        }
                    }
                }

                results.sort((a, b) => (a.timestamp ?? 0) - (b.timestamp ?? 0));
                res.json({
                    results,
                    scanned,
                    truncated: truncated || scanned < totalSteps,
                });
                return;
            }
        }
        let offset = 0;
        let scanned = 0;
        let truncated = false;

        while (scanned < MARKET_SCAN_LIMIT) {
            const batch = await fetchGammaMarketsPage(MARKET_SCAN_PAGE_SIZE, offset);
            if (batch.length === 0) {
                break;
            }

            scanned += batch.length;

            for (const market of batch) {
                const slug = market.slug ?? '';
                if (!slug) {
                    continue;
                }
                const slugLower = slug.toLowerCase();
                if (!slugLower.startsWith(prefix)) {
                    continue;
                }

                const marketTs = getMarketTimestampSeconds(market);
                if (startTs !== null || endTs !== null) {
                    if (!isTimestampInRange(marketTs, startTs, endTs)) {
                        continue;
                    }
                }

                const endDate = market.endDateIso ?? market.endDate ?? undefined;
                for (const choice of buildMarketTokenChoices(market)) {
                    const key = `${slug}-${choice.tokenId}-${choice.outcomeIndex ?? ''}`;
                    if (seen.has(key)) {
                        continue;
                    }
                    seen.add(key);
                    results.push({
                        slug,
                        tokenId: choice.tokenId,
                        outcome: choice.outcome,
                        outcomeIndex: choice.outcomeIndex,
                        endDate,
                        timestamp: marketTs ?? undefined,
                    });
                    if (results.length >= limit) {
                        truncated = true;
                        break;
                    }
                }
                if (results.length >= limit) {
                    break;
                }
            }

            if (results.length >= limit) {
                break;
            }
            if (batch.length < MARKET_SCAN_PAGE_SIZE) {
                break;
            }
            offset += batch.length;
        }

        results.sort((a, b) => (a.timestamp ?? 0) - (b.timestamp ?? 0));
        res.json({ results, scanned, truncated });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/resolve-latest', async (req: Request, res: Response) => {
    const prefixInput = typeof req.query.prefix === 'string' ? req.query.prefix.trim() : '';
    const prefixRaw = prefixInput.toLowerCase();
    const prefix = stripSlugTimestamp(prefixRaw);
    if (!prefix) {
        res.status(400).json({ error: 'prefix is required' });
        return;
    }

    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);

    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    try {
        const intervalSeconds = parsePrefixIntervalSeconds(prefix);
        if (intervalSeconds) {
            const nowTs = Math.floor(Date.now() / 1000);
            const alignedNow = alignTimestampToInterval(nowTs, intervalSeconds, 'floor');
            const maxChecks = 12;
            let bestMarket: GammaMarket | null = null;

            for (let step = 0; step < maxChecks; step += 1) {
                const ts = alignedNow - step * intervalSeconds;
                if (startTs !== null || endTs !== null) {
                    if (!isTimestampInRange(ts, startTs, endTs)) {
                        continue;
                    }
                }
                const slug = `${prefix}-${ts}`;
                const markets = await fetchGammaMarketsBySlug(slug);
                if (markets.length > 0) {
                    bestMarket = markets[0];
                    break;
                }
            }

            if (bestMarket && bestMarket.slug) {
                const choicesMap = new Map<string, MarketTokenChoice>();
                for (const choice of buildMarketTokenChoices(bestMarket)) {
                    if (!choicesMap.has(choice.tokenId)) {
                        choicesMap.set(choice.tokenId, choice);
                    }
                }
                const choices = Array.from(choicesMap.values());
                res.json({
                    market: {
                        slug: bestMarket.slug,
                        endDate: bestMarket.endDateIso ?? bestMarket.endDate ?? undefined,
                    },
                    options: choices,
                });
                return;
            }
        }

        let offset = 0;
        let scanned = 0;
        let bestMarket: GammaMarket | null = null;
        let bestTimestamp: number | null = null;

        while (scanned < MARKET_SCAN_LIMIT) {
            const batch = await fetchGammaMarketsPage(MARKET_SCAN_PAGE_SIZE, offset);
            if (batch.length === 0) {
                break;
            }

            scanned += batch.length;

            for (const market of batch) {
                const slug = market.slug ?? '';
                if (!slug) {
                    continue;
                }
                if (!slug.toLowerCase().startsWith(prefix)) {
                    continue;
                }

                const marketTs = getMarketTimestampSeconds(market);
                if (startTs !== null || endTs !== null) {
                    if (!isTimestampInRange(marketTs, startTs, endTs)) {
                        continue;
                    }
                }

                if (marketTs === null) {
                    if (!bestMarket) {
                        bestMarket = market;
                        bestTimestamp = null;
                    }
                    continue;
                }

                if (bestTimestamp === null || marketTs > bestTimestamp) {
                    bestMarket = market;
                    bestTimestamp = marketTs;
                }
            }

            if (batch.length < MARKET_SCAN_PAGE_SIZE) {
                break;
            }
            offset += batch.length;
        }

        if (!bestMarket || !bestMarket.slug) {
            res.status(404).json({ error: 'No markets matched this prefix' });
            return;
        }

        const choicesMap = new Map<string, MarketTokenChoice>();
        for (const choice of buildMarketTokenChoices(bestMarket)) {
            if (!choicesMap.has(choice.tokenId)) {
                choicesMap.set(choice.tokenId, choice);
            }
        }

        res.json({
            slug: bestMarket.slug,
            endDate: bestMarket.endDateIso ?? bestMarket.endDate ?? undefined,
            timestamp: bestTimestamp ?? undefined,
            options: Array.from(choicesMap.values()),
            scanned,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/search', async (req: Request, res: Response) => {
    const query = ((req.query.query as string) || '').trim().toLowerCase();
    const status = ((req.query.status as string) || 'active').toLowerCase();
    const limitRaw = parseInt((req.query.limit as string) || '50', 10);
    const limit = Number.isNaN(limitRaw) ? 50 : Math.min(Math.max(limitRaw, 1), 500);
    const offsetRaw = parseInt((req.query.offset as string) || '0', 10);
    const offset = Number.isNaN(offsetRaw) ? 0 : Math.max(offsetRaw, 0);

    try {
        const results: MarketSearchEntry[] = [];
        let scanned = 0;
        let currentOffset = offset;
        const pageSize = Math.min(Math.max(limit, 1), MARKET_SCAN_PAGE_SIZE);

        while (results.length < limit && scanned < MARKET_SCAN_LIMIT) {
            const batch = await fetchGammaMarketsPage(pageSize, currentOffset);
            if (!batch.length) {
                break;
            }
            scanned += batch.length;
            currentOffset += batch.length;

            const filtered = batch.filter((market: any) => {
                const question = (market.question ?? '').toLowerCase();
                const slug = (market.slug ?? '').toLowerCase();
                const category = (market.category ?? market?.events?.[0]?.category ?? '').toLowerCase();
                const matchesQuery = query
                    ? question.includes(query) || slug.includes(query) || category.includes(query)
                    : true;

                if (!matchesQuery) {
                    return false;
                }

                if (status === 'active') {
                    return market.active === true && market.closed !== true;
                }
                if (status === 'closed') {
                    return market.closed === true;
                }
                return true;
            });

            filtered.forEach((market) => {
                if (results.length < limit) {
                    results.push(mapMarketSearchEntry(market));
                }
            });

            if (batch.length < pageSize) {
                break;
            }
        }

        res.json({
            markets: results,
            scanned,
            nextOffset: currentOffset,
            exhausted: scanned >= MARKET_SCAN_LIMIT,
        });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets', async (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || '8', 10), 50);
    const query = ((req.query.query as string) || '').trim().toLowerCase();

    try {
        const markets = await getMarketPool();
        const filtered = query
            ? markets.filter((market) => {
                  const question = market.question?.toLowerCase() || '';
                  const slug = market.slug?.toLowerCase() || '';
                  return question.includes(query) || slug.includes(query);
              })
            : markets;

        const results = filtered.slice(0, limit).map(mapMarket);
        res.json({ markets: results });
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.get('/api/markets/series', async (req: Request, res: Response) => {
    const seriesId = typeof req.query.seriesId === 'string' ? req.query.seriesId.trim() : '';
    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);
    const limitRaw = parseInt((req.query.limit as string) || '200', 10);
    const limit = Number.isNaN(limitRaw) ? 200 : Math.min(Math.max(limitRaw, 1), 2000);

    if (!seriesId) {
        res.status(400).json({ error: 'seriesId is required' });
        return;
    }
    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    try {
        const results: MarketSeriesEntry[] = [];
        let offset = 0;
        let scanned = 0;
        let truncated = false;
        const pageSize = 100;

        while (results.length < limit && scanned < MARKET_SCAN_LIMIT) {
            const batch = await fetchGammaMarketsBySeriesId(seriesId, pageSize, offset);
            if (batch.length === 0) {
                break;
            }

            scanned += batch.length;
            offset += batch.length;

            for (const market of batch) {
                const marketTs = getMarketTimestampSeconds(market);
                if (!isTimestampInRange(marketTs, startTs, endTs)) {
                    continue;
                }
                results.push(mapMarketSeriesEntry(market));
                if (results.length >= limit) {
                    truncated = true;
                    break;
                }
            }

            if (batch.length < pageSize) {
                break;
            }
        }

        results.sort((a, b) => (a.timestamp ?? 0) - (b.timestamp ?? 0));
        res.json({ markets: results, scanned, truncated });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/markets/trades', async (req: Request, res: Response) => {
    const market = typeof req.query.market === 'string' ? req.query.market.trim() : '';
    const startParam = req.query.startTs ?? req.query.start;
    const endParam = req.query.endTs ?? req.query.end;
    const startTs = parseTimestampInput(startParam);
    const endTs = parseTimestampInput(endParam);
    const limitRaw = parseInt((req.query.limit as string) || '500', 10);
    const limit = Number.isNaN(limitRaw) ? 500 : Math.min(Math.max(limitRaw, 1), 500);

    if (!market) {
        res.status(400).json({ error: 'market is required' });
        return;
    }
    if (isQueryValuePresent(startParam) && startTs === null) {
        res.status(400).json({ error: 'Invalid start timestamp' });
        return;
    }
    if (isQueryValuePresent(endParam) && endTs === null) {
        res.status(400).json({ error: 'Invalid end timestamp' });
        return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
        res.status(400).json({ error: 'startTs must be less than or equal to endTs' });
        return;
    }

    try {
        const url = `https://data-api.polymarket.com/trades?market=${encodeURIComponent(
            market
        )}&limit=${limit}`;
        const data = await fetchData(url);
        const list = Array.isArray(data) ? data : [];
        const filtered = list.filter((trade: any) => {
            const timestamp = getTradeTimestampSeconds(trade);
            return isTimestampInRange(timestamp, startTs, endTs);
        });
        const trades = filtered.map((trade: any) => mapMarketTradeEntry(trade, market));
        res.json({ trades, total: list.length, filtered: trades.length });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/leaderboard', async (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || '5', 10), 20);

    try {
        const markets = await getMarketPool();
        const ranked = markets
            .map(mapMarket)
            .sort((a, b) => b.volume24hr - a.volume24hr)
            .slice(0, limit)
            .map((market, index) => ({
                rank: index + 1,
                id: market.id,
                question: market.question,
                volume24hr: market.volume24hr,
                slug: market.slug,
            }));

        res.json({ leaders: ranked });
    } catch (error) {
        res.status(500).json({
            error: error instanceof Error ? error.message : String(error),
        });
    }
});

app.get('/api/binance/ticker', async (req: Request, res: Response) => {
    const symbol = typeof req.query.symbol === 'string' ? req.query.symbol.toUpperCase() : '';

    if (!isValidBinanceSymbol(symbol)) {
        res.status(400).json({ error: 'Invalid symbol' });
        return;
    }

    try {
        const data = await fetchData(`${BINANCE_API}/ticker/24hr?symbol=${symbol}`);
        res.json({ ticker: data });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.get('/api/binance/klines', async (req: Request, res: Response) => {
    const symbol = typeof req.query.symbol === 'string' ? req.query.symbol.toUpperCase() : '';
    const interval = typeof req.query.interval === 'string' ? req.query.interval : '1m';
    const limit = Math.min(parseInt((req.query.limit as string) || '120', 10), 1000);

    if (!isValidBinanceSymbol(symbol)) {
        res.status(400).json({ error: 'Invalid symbol' });
        return;
    }
    if (!isValidBinanceInterval(interval)) {
        res.status(400).json({ error: 'Invalid interval' });
        return;
    }

    try {
        const data = await fetchData(
            `${BINANCE_API}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
        );
        res.json({ klines: data });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

app.post('/api/order', requireAuth, async (req: Request, res: Response) => {
    if (ANALYSIS_ONLY) {
        res.status(403).json({ error: 'Trading is disabled in analysis mode' });
        return;
    }
    const tokenId = typeof req.body?.tokenId === 'string' ? req.body.tokenId : '';
    const sideInput = typeof req.body?.side === 'string' ? req.body.side.toLowerCase() : '';
    const amountRaw = req.body?.amount;
    const amount = typeof amountRaw === 'number' ? amountRaw : parseFloat(amountRaw);

    if (!tokenId) {
        res.status(400).json({ error: 'tokenId is required' });
        return;
    }
    if (sideInput !== 'buy' && sideInput !== 'sell') {
        res.status(400).json({ error: 'side must be buy or sell' });
        return;
    }
    if (!amount || Number.isNaN(amount) || amount <= 0) {
        res.status(400).json({ error: 'amount must be a positive number' });
        return;
    }

    try {
        const clobClient = await ensureClobClient();
        const orderBook = await clobClient.getOrderBook(tokenId);

        if (sideInput === 'buy') {
            if (!orderBook.asks || orderBook.asks.length === 0) {
                res.status(400).json({ error: 'No asks available for this market' });
                return;
            }
            if (amount < MIN_ORDER_SIZE_USD) {
                res.status(400).json({ error: 'Minimum buy is $1.00' });
                return;
            }

            const bestAsk = orderBook.asks.reduce((min, ask) => {
                return parseFloat(ask.price) < parseFloat(min.price) ? ask : min;
            }, orderBook.asks[0]);
            const price = parseFloat(bestAsk.price);
            const maxUsd = parseFloat(bestAsk.size) * price;
            const orderUsd = Math.min(amount, maxUsd);

            const signedOrder = await clobClient.createMarketOrder({
                side: Side.BUY,
                tokenID: tokenId,
                amount: orderUsd,
                price,
            });
            const resp = await clobClient.postOrder(signedOrder, OrderType.FOK);
            res.json({ success: resp.success, response: resp, price, amount: orderUsd });
            return;
        }

        if (!orderBook.bids || orderBook.bids.length === 0) {
            res.status(400).json({ error: 'No bids available for this market' });
            return;
        }
        if (amount < MIN_ORDER_SIZE_TOKENS) {
            res.status(400).json({ error: 'Minimum sell is 1 token' });
            return;
        }

        const bestBid = orderBook.bids.reduce((max, bid) => {
            return parseFloat(bid.price) > parseFloat(max.price) ? bid : max;
        }, orderBook.bids[0]);
        const price = parseFloat(bestBid.price);
        const sellAmount = Math.min(amount, parseFloat(bestBid.size));

        const signedOrder = await clobClient.createMarketOrder({
            side: Side.SELL,
            tokenID: tokenId,
            amount: sellAmount,
            price,
        });
        const resp = await clobClient.postOrder(signedOrder, OrderType.FOK);
        res.json({ success: resp.success, response: resp, price, amount: sellAmount });
    } catch (error) {
        res.status(500).json({ error: error instanceof Error ? error.message : String(error) });
    }
});

if (fs.existsSync(dashboardDist)) {
    app.use(express.static(dashboardDist));
}

app.get('*', (_req: Request, res: Response) => {
    if (fs.existsSync(dashboardIndex)) {
        res.sendFile(dashboardIndex);
        return;
    }

    res.status(404).send('Dashboard build not found. Run: npm --prefix dashboard run build');
});

const server = app.listen(PORT, HOST, async () => {
    Logger.success(`Dashboard server running at http://${HOST}:${PORT}`);
    if (AUTO_START_BOT && !ANALYSIS_ONLY) {
        try {
            await startBot({ showWelcome: false, runHealthCheck: false });
        } catch (error) {
            Logger.error(`Failed to auto-start bot: ${error}`);
        }
    } else if (AUTO_START_BOT && ANALYSIS_ONLY) {
        Logger.warning('Analysis mode enabled - auto-start bot is disabled.');
    }
});

const shutdown = async (signal: string) => {
    Logger.warning(`Received ${signal}, shutting down server...`);

    server.close(async () => {
        try {
            await stopBot({ closeDb: true });
        } catch (error) {
            Logger.error(`Error while stopping bot: ${error}`);
        } finally {
            process.exit(0);
        }
    });
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
