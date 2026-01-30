import { FormEvent, useEffect, useMemo, useState, MouseEvent, useRef } from 'react';
import * as XLSX from 'xlsx';

declare global {
  interface Window {
    grecaptcha?: {
      ready: (callback: () => void) => void;
      execute: (siteKey: string, options: { action: string }) => Promise<string>;
    };
  }
}

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
  price?: number | string;
  size?: number | string;
  usdcSize?: number | string;
  timestamp?: number;
  transactionHash?: string;
  category?: string | null;
  tags?: string[];
};

type PositionHistoryEntry = {
  key: string;
  asset: string;
  market: string;
  outcome: string;
  side: string;
  tradePrice: number;
  tradeSize: number;
  tradeUsdc: number;
  timestamp: number;
  timestampIso?: string;
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
  lastTradeAt?: number | null;
  lastTradeIso?: string;
  status: 'OPEN' | 'CLOSED';
};

type TradeRow = {
  id: string;
  market: string;
  value: string;
  wallet: string;
  time: string;
  side: 'buy' | 'sell';
};

type PriceHistoryPoint = {
  timestamp: number;
  price: number;
};

type PriceHistoryMeta = {
  tokenId: string;
  slug?: string;
  outcome?: string;
  outcomeIndex?: number;
  priceToBeat?: number | null;
  currentPrice?: number | null;
  liquidityClob?: number | null;
};

type PriceHistoryOption = {
  tokenId: string;
  outcome?: string;
  outcomeIndex?: number;
};

type PriceHistorySeries = {
  tokenId: string;
  slug?: string;
  outcome?: string;
  outcomeIndex?: number;
  priceToBeat?: number | null;
  currentPrice?: number | null;
  liquidityClob?: number | null;
  points: PriceHistoryPoint[];
};

type PositionItem = {
  asset?: string;
  title?: string;
  slug?: string;
  eventSlug?: string;
  outcome?: string;
  size?: number;
  avgPrice?: number;
  currentValue?: number;
  cashPnl?: number;
  realizedPnl?: number;
  totalBought?: number;
  percentPnl?: number;
  endDate?: string;
  timestamp?: number;
};

type ResolvedMarket = {
  slug: string;
  tokenId: string;
  outcome?: string;
  outcomeIndex?: number;
  endDate?: string;
  timestamp?: number;
};

const escapeCsvValue = (value: unknown) => {
  const text = value === null || value === undefined ? '' : String(value);
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
};

type Market = {
  id: number;
  question: string;
  slug?: string;
  category?: string | null;
  tags?: string[];
  endDate?: string | null;
  volume24hr?: number;
  volume?: number;
  openInterest?: number | null;
  liquidity: number;
  lastTradePrice?: number;
  description?: string | null;
  outcomes: string[];
  outcomePrices: number[];
  clobTokenIds: string[];
  conditionId?: string;
  active?: boolean;
  closed?: boolean;
  source?: string;
  icon?: string;
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

type LiveStreamBookStats = {
  tokenId: string;
  bestBid?: number;
  bestAsk?: number;
  mid?: number;
  spread?: number;
  updatedAt: number;
};

type LiveStreamPriceHistory = {
  points: number[];
  bidPoints: number[];
  askPoints: number[];
  updatedAt: number;
};

type LiveStreamPriceSnapshotEntry = {
  timestampMs: number;
  priceToBeat: number | null;
  currentPrice: number | null;
};

type LiveStreamMessage = {
  id: string;
  timestamp: number;
  channel?: string;
  tokenId?: string;
  price?: number | null;
  summary: string;
  raw: string;
  rawFull: string;
};

type LeaderEntry = {
  rank: number;
  id: number;
  question: string;
  volume24hr: number;
  slug?: string;
};

type WalletSummary = {
  wallet: string;
  balance: number;
  positionsValue: number;
  portfolioValue: number;
  totalInitialValue: number;
  cashPnl: number;
  totalProfit?: number;
  totalLoss?: number;
  pnlPercent: number;
  positionsCount: number;
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

type TraderProfileResponse = {
  summary: TraderProfileSummary;
  trades: TradeActivity[];
  positions: PositionSummaryEntry[];
  positionHistory: PositionHistoryEntry[];
  dailyPnl: DailyTradePnlEntry[];
  categories: CategorySummaryEntry[];
  range?: {
    startTs?: number | null;
    endTs?: number | null;
    limit?: number;
  };
};

type TraderScanEntry = {
  address: string;
  tradeCount: number;
  volume: number;
  lastTradeAt: number | null;
};

type LifetimeSummary = {
  totalProfit: number;
  totalLoss: number;
  netPnl: number;
  tradesCount: number;
  eventsCount: number;
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

type AuthUser = {
  id: string;
  email: string;
  role: 'admin' | 'user';
  walletAddress?: string;
  createdAt?: string | null;
  lastLoginAt?: string | null;
};

type AdminActivity = {
  id: string;
  action: string;
  createdAt: string | null;
  user: { id: string; email: string; role: string } | null;
  metadata?: Record<string, unknown>;
};

const RECAPTCHA_SITE_KEY = import.meta.env.VITE_RECAPTCHA_SITE_KEY as
  | string
  | undefined;
const RECAPTCHA_ENABLED = (import.meta.env.VITE_RECAPTCHA_ENABLED ?? 'true') === 'true';

const EXPORT_BATCH_SIZE = 500;
const EXPORT_MAX_TRADES = 100000;
const RANGE_EXPORT_CHUNK_SIZE = 50000;
const RANGE_PREVIEW_LIMIT = 200;
const PRICE_HISTORY_TABLE_LIMIT = 50;
const LIVE_STREAM_POLL_INTERVAL_MS = 5000;
const LIVE_STREAM_ARCHIVE_LIMIT = 20000;
const LIVE_STREAM_ADVANCE_BUFFER_MS = 2000;
const LIVE_STREAM_PRICE_POINTS = 120;
const LIVE_STREAM_SNAPSHOT_INTERVAL_MS = 30000;
const LIVE_STREAM_STORAGE_KEY = 'liveStreamAutoState';
type LiveStreamChannelState = Record<string, boolean>;
const LIVE_STREAM_CHANNEL_DEFAULTS: LiveStreamChannelState = {
  market: true,
  book: true,
};
const getLiveStreamChannelList = (state: LiveStreamChannelState) =>
  Object.entries(state)
    .filter(([, enabled]) => enabled)
    .map(([channel]) => channel);
const buildLiveStreamChannelState = (
  channels?: string[]
): LiveStreamChannelState => {
  if (!channels) {
    return { ...LIVE_STREAM_CHANNEL_DEFAULTS };
  }
  const keys = Array.from(
    new Set<string>([
      ...Object.keys(LIVE_STREAM_CHANNEL_DEFAULTS),
      ...channels,
    ])
  );
  const state: LiveStreamChannelState = {};
  keys.forEach((key) => {
    state[key] = channels.includes(key);
  });
  return state;
};

const formatAddress = (address?: string) => {
  if (!address) {
    return '0x----';
  }
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
};

const parseNumber = (value?: number | string) => {
  if (value === undefined || value === null) {
    return 0;
  }
  if (typeof value === 'number') {
    return value;
  }
  const parsed = parseFloat(value);
  return Number.isNaN(parsed) ? 0 : parsed;
};

const parseFiniteNumber = (value: unknown) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = typeof value === 'number' ? value : parseFloat(String(value));
  return Number.isFinite(parsed) ? parsed : null;
};

const formatUsd = (value?: number | string) => {
  const num = parseNumber(value);
  if (!num) {
    return '$0.00';
  }
  return `$${num.toFixed(2)}`;
};

const formatUsdOptional = (value?: number | null) => {
  if (value === null || value === undefined) {
    return '--';
  }
  return formatUsd(value);
};

const formatTokenPrice = (value?: number | string | null) => {
  if (value === null || value === undefined) {
    return '--';
  }
  const num = parseNumber(value);
  return `$${num.toFixed(4)}`;
};

const formatPercent = (value?: number | null) => {
  if (value === null || value === undefined) {
    return '--';
  }
  return `${(value * 100).toFixed(2)}%`;
};

const formatPercentNumber = (value?: number | null) => {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--';
  }
  return `${value.toFixed(2)}%`;
};

const formatShares = (value?: number | string) => {
  const num = parseNumber(value);
  if (!num) {
    return '0';
  }
  return num.toFixed(2);
};

const getPositionPercent = (position: PositionItem) => {
  if (position.percentPnl !== undefined && position.percentPnl !== null) {
    return position.percentPnl;
  }
  const base = parseNumber(position.totalBought);
  const pnl = parseNumber(position.realizedPnl ?? position.cashPnl);
  if (!base) {
    return null;
  }
  return (pnl / base) * 100;
};

const formatPositionPnl = (position: PositionItem) => {
  const pnlValue = position.cashPnl ?? position.realizedPnl;
  const pctLabel = formatPercentNumber(getPositionPercent(position));
  const usdLabel = formatUsd(pnlValue);
  return pctLabel === '--' ? usdLabel : `${usdLabel} (${pctLabel})`;
};

const getPnlClass = (value?: number | null) => {
  if (value === null || value === undefined) {
    return 'value-neutral';
  }
  if (value > 0) {
    return 'value-positive';
  }
  if (value < 0) {
    return 'value-negative';
  }
  return 'value-neutral';
};

const formatRatio = (value?: number | null) => {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--';
  }
  return value.toFixed(2);
};

const formatCompactUsd = (value?: number | string) => {
  const num = parseNumber(value);
  if (!num) {
    return '$0';
  }
  if (num >= 1_000_000) {
    return `$${(num / 1_000_000).toFixed(2)}M`;
  }
  if (num >= 1_000) {
    return `$${(num / 1_000).toFixed(2)}k`;
  }
  return `$${num.toFixed(2)}`;
};

const formatTimestamp = (timestamp?: number) => {
  if (!timestamp) {
    return 'Unknown time';
  }
  const normalized = timestamp < 1_000_000_000_000 ? timestamp * 1000 : timestamp;
  return new Date(normalized).toLocaleString();
};

const formatTimestampIso = (timestamp?: number) => {
  if (!timestamp) {
    return '';
  }
  const normalized = timestamp < 1_000_000_000_000 ? timestamp * 1000 : timestamp;
  return new Date(normalized).toISOString();
};

const formatDateLabel = (value?: string | null) => {
  if (!value) {
    return '--';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleDateString();
};

const formatIso = (value?: string | null) => {
  if (!value) {
    return '--';
  }
  return new Date(value).toLocaleString();
};

const formatMetadata = (metadata?: Record<string, unknown>) => {
  if (!metadata || Object.keys(metadata).length === 0) {
    return '--';
  }
  try {
    const text = JSON.stringify(metadata);
    return text.length > 140 ? `${text.slice(0, 137)}...` : text;
  } catch (_error) {
    return '[metadata]';
  }
};

const executeRecaptcha = async (action: string) => {
  if (!RECAPTCHA_ENABLED || !RECAPTCHA_SITE_KEY) {
    return '';
  }
  if (!window.grecaptcha) {
    throw new Error('reCAPTCHA not ready');
  }

  return new Promise<string>((resolve, reject) => {
    window.grecaptcha?.ready(() => {
      window.grecaptcha
        ?.execute(RECAPTCHA_SITE_KEY, { action })
        .then(resolve)
        .catch(reject);
    });
  });
};

const normalizeTimestamp = (timestamp?: number) => {
  if (!timestamp) {
    return 0;
  }
  return timestamp < 1_000_000_000_000 ? timestamp * 1000 : timestamp;
};

const normalizeTimestampSeconds = (timestamp?: number) => {
  if (!timestamp) {
    return 0;
  }
  return timestamp >= 1_000_000_000_000 ? Math.floor(timestamp / 1000) : Math.floor(timestamp);
};

const stripSlugTimestamp = (slug: string) => slug.replace(/-\d+$/, '');
const DEFAULT_RESOLVE_LIMIT = 200;
const MAX_RESOLVE_LIMIT = 2000;
const DEFAULT_OUTCOME_COUNT = 2;

const parsePrefixIntervalSeconds = (prefix: string) => {
  const cleaned = stripSlugTimestamp(prefix.toLowerCase());
  const match = cleaned.match(/-(\d+)(m|h|d)$/);
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

const estimateResolveLimit = (
  prefix: string,
  startTs: number | null,
  endTs: number | null
) => {
  if (startTs === null || endTs === null) {
    return DEFAULT_RESOLVE_LIMIT;
  }
  const intervalSeconds = parsePrefixIntervalSeconds(prefix);
  if (!intervalSeconds) {
    return DEFAULT_RESOLVE_LIMIT;
  }
  const rangeSeconds = Math.max(0, endTs - startTs);
  const markets = Math.floor(rangeSeconds / intervalSeconds) + 1;
  const estimate = Math.max(markets * DEFAULT_OUTCOME_COUNT, DEFAULT_OUTCOME_COUNT);
  return Math.min(estimate, MAX_RESOLVE_LIMIT);
};

const parseMarketSlugSequence = (slug: string) => {
  const trimmed = slug.trim();
  if (!trimmed) {
    return null;
  }
  const match = trimmed.match(/^(.*)-(\d+)$/);
  if (!match) {
    return null;
  }
  const prefix = match[1];
  const timestamp = parseInt(match[2], 10);
  if (!Number.isFinite(timestamp) || timestamp <= 0) {
    return null;
  }
  const intervalSeconds = parsePrefixIntervalSeconds(trimmed);
  if (!intervalSeconds) {
    return null;
  }
  return { prefix, timestamp, intervalSeconds };
};

const extractSlugTimestamp = (slug: string) => {
  const match = slug.trim().match(/-(\d+)$/);
  if (!match) {
    return null;
  }
  const timestamp = parseInt(match[1], 10);
  return Number.isFinite(timestamp) ? timestamp : null;
};

const alignTimestampToInterval = (timestamp: number, intervalSeconds: number) =>
  Math.floor(timestamp / intervalSeconds) * intervalSeconds;

const normalizeSlugTimestamp = (slug: string) => {
  const trimmed = slug.trim();
  const match = trimmed.match(/^(.*)-(\d+)$/);
  if (!match) {
    return { slug: trimmed, changed: false };
  }
  const prefix = match[1];
  let timestamp = parseInt(match[2], 10);
  if (!Number.isFinite(timestamp) || timestamp <= 0) {
    return { slug: trimmed, changed: false };
  }
  let adjusted = timestamp;
  while (adjusted > 4_000_000_000) {
    adjusted = Math.floor(adjusted / 10);
  }
  if (adjusted !== timestamp && adjusted > 0) {
    return { slug: `${prefix}-${adjusted}`, changed: true };
  }
  return { slug: trimmed, changed: false };
};

const pickLatestResolvedMarket = (results: ResolvedMarket[]) => {
  const grouped = new Map<
    string,
    { timestamp: number; tokens: Set<string> }
  >();
  results.forEach((entry) => {
    const slug = entry.slug;
    const ts = entry.timestamp ?? extractSlugTimestamp(slug) ?? 0;
    const existing = grouped.get(slug);
    if (!existing || ts > existing.timestamp) {
      grouped.set(slug, {
        timestamp: ts,
        tokens: new Set(existing?.tokens ?? []),
      });
    }
    const bucket = grouped.get(slug);
    if (bucket) {
      bucket.tokens.add(entry.tokenId);
    }
  });
  let latestSlug: string | null = null;
  let latestTimestamp = -1;
  grouped.forEach((value, slug) => {
    if (value.timestamp > latestTimestamp) {
      latestTimestamp = value.timestamp;
      latestSlug = slug;
    }
  });
  if (!latestSlug) {
    return null;
  }
  const tokens = Array.from(grouped.get(latestSlug)?.tokens ?? []);
  return tokens.length > 0 ? { slug: latestSlug, tokens } : null;
};

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const parseTimestampInput = (value: string) => {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const numeric = Number(trimmed);
  if (Number.isFinite(numeric)) {
    return normalizeTimestampSeconds(numeric);
  }
  const parsed = Date.parse(trimmed);
  if (!Number.isNaN(parsed)) {
    return Math.floor(parsed / 1000);
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

const normalizeSide = (side?: string): 'buy' | 'sell' =>
  side && side.toLowerCase() === 'sell' ? 'sell' : 'buy';

const computeTradeTotals = (trades: TradeActivity[]) => {
  let buyCount = 0;
  let sellCount = 0;
  let buyUsd = 0;
  let sellUsd = 0;

  trades.forEach((trade) => {
    const side = normalizeSide(trade.side);
    const usdc = parseNumber(trade.usdcSize);
    if (side === 'buy') {
      buyCount += 1;
      buyUsd += usdc;
    } else {
      sellCount += 1;
      sellUsd += usdc;
    }
  });

  return {
    buyCount,
    sellCount,
    buyUsd,
    sellUsd,
    netUsd: sellUsd - buyUsd,
  };
};

const mapTrades = (trades: TradeActivity[]): TradeRow[] =>
  trades.map((trade) => ({
    id: trade.id,
    market: trade.title || trade.slug || trade.eventSlug || 'Market update',
    value: formatUsd(trade.usdcSize),
    wallet: formatAddress(trade.userAddress),
    time: formatTimestamp(trade.timestamp),
    side: normalizeSide(trade.side),
  }));

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
      timestamp: normalizeTimestamp(trade.timestamp),
    }))
    .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0) || a.index - b.index)
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

    const timestamp = trade.timestamp ?? 0;
    history.push({
      key,
      asset: existing.asset || trade.asset || '',
      market: existing.market || trade.title || trade.market || trade.slug || trade.eventSlug || '',
      outcome: existing.outcome || trade.outcome || '',
      side,
      tradePrice,
      tradeSize,
      tradeUsdc,
      timestamp,
      timestampIso: formatTimestampIso(timestamp),
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
      timestamp: normalizeTimestamp(trade.timestamp),
    }))
    .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0) || a.index - b.index)
    .map((entry) => entry.trade);

  const positions = new Map<
    string,
    {
      key: string;
      asset: string;
      market: string;
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
      lastTradeIso: string;
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
      lastTradeIso: '',
    };

    if (tradePrice > 0) {
      existing.lastPrice = tradePrice;
    }
    if (trade.timestamp) {
      existing.lastTradeAt = trade.timestamp;
      existing.lastTradeIso = formatTimestampIso(trade.timestamp);
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
      lastTradeIso: pos.lastTradeIso,
      status: pos.openSize > 0 ? 'OPEN' : 'CLOSED',
    };
  });
};

const formatSheetNumber = (value: number | null | undefined) =>
  value !== null && value !== undefined && Number.isFinite(value) ? value : '';

const buildWalletExportWorkbook = (
  wallet: string,
  summary: WalletSummary | null,
  trades: TradeActivity[],
  summaryError?: string,
  warning?: string,
  truncated?: boolean,
  positionHistory?: PositionHistoryEntry[],
  positionSummary?: PositionSummaryEntry[],
  lifetimeSummary?: LifetimeSummary | null,
  lifetimeError?: string
) => {
  const workbook = XLSX.utils.book_new();

  const overview: Array<Array<string | number>> = [['Metric', 'Value']];
  const pushOverview = (label: string, value: string | number | null | undefined) => {
    overview.push([label, value ?? '']);
  };

  pushOverview('Exported At', new Date().toISOString());
  pushOverview('Wallet', wallet);
  pushOverview('Trades Exported', trades.length);
  pushOverview('Summary Status', summary ? 'ok' : 'unavailable');
  if (summaryError) {
    pushOverview('Summary Error', summaryError);
  }
  pushOverview('Lifetime Status', lifetimeSummary ? 'ok' : 'unavailable');
  if (lifetimeError) {
    pushOverview('Lifetime Error', lifetimeError);
  }
  if (warning) {
    pushOverview('Export Warning', warning);
  }
  if (truncated) {
    pushOverview('Export Truncated', 'true');
  }
  if (summary) {
    pushOverview('Balance', summary.balance);
    pushOverview('Positions Value', summary.positionsValue);
    pushOverview('Portfolio Value', summary.portfolioValue);
    pushOverview('Total Initial Value', summary.totalInitialValue);
    pushOverview('Cash PnL', summary.cashPnl);
    pushOverview('PnL Percent', summary.pnlPercent);
    pushOverview('Open Positions', summary.positionsCount);
  }
  if (lifetimeSummary) {
    pushOverview('Lifetime Net PnL', lifetimeSummary.netPnl);
    pushOverview('Lifetime Profit', lifetimeSummary.totalProfit);
    pushOverview('Lifetime Loss', lifetimeSummary.totalLoss);
    pushOverview('Lifetime 24h Net', lifetimeSummary.lastDayNet ?? '');
    pushOverview('Lifetime Win Rate', lifetimeSummary.winRate ?? '');
    pushOverview('Lifetime Profit Factor', lifetimeSummary.profitFactor ?? '');
    pushOverview('Avg Win Day', lifetimeSummary.avgWinDay ?? '');
    pushOverview('Avg Loss Day', lifetimeSummary.avgLossDay ?? '');
    pushOverview('Max Drawdown', lifetimeSummary.maxDrawdown ?? '');
    pushOverview('Sharpe Ratio', lifetimeSummary.sharpeRatio ?? '');
    pushOverview('Active Days', lifetimeSummary.activeDays ?? '');
    pushOverview('Lifetime Activity Count', lifetimeSummary.eventsCount);
    if (lifetimeSummary.firstTradeAt) {
      pushOverview('Lifetime First Trade', formatTimestampIso(lifetimeSummary.firstTradeAt));
    }
    if (lifetimeSummary.lastTradeAt) {
      pushOverview('Lifetime Last Trade', formatTimestampIso(lifetimeSummary.lastTradeAt));
    }
    if (lifetimeSummary.truncated) {
      pushOverview('Lifetime Truncated', 'true');
    }
  }

  const overviewSheet = XLSX.utils.aoa_to_sheet(overview);
  XLSX.utils.book_append_sheet(workbook, overviewSheet, 'Overview');

  const activityRows: Array<Array<string | number>> = [
    [
      'Trade ID',
      'User Address',
      'Title',
      'Slug',
      'Event Slug',
      'Side',
      'Outcome',
      'Price',
      'Size',
      'USDC Size',
      'Timestamp',
      'Timestamp ISO',
      'Transaction Hash',
    ],
  ];
  trades.forEach((trade) => {
    activityRows.push([
      trade.id,
      trade.userAddress,
      trade.title ?? '',
      trade.slug ?? '',
      trade.eventSlug ?? '',
      trade.side ?? '',
      trade.outcome ?? '',
      formatSheetNumber(parseNumber(trade.price)),
      formatSheetNumber(parseNumber(trade.size)),
      formatSheetNumber(parseNumber(trade.usdcSize)),
      trade.timestamp ?? '',
      formatTimestampIso(trade.timestamp),
      trade.transactionHash ?? '',
    ]);
  });
  const activitySheet = XLSX.utils.aoa_to_sheet(activityRows);
  XLSX.utils.book_append_sheet(workbook, activitySheet, 'Wallet Activity');

  const pnlRows: Array<Array<string | number>> = [];
  if (positionSummary && positionSummary.length > 0) {
    pnlRows.push(['Position Summary (entry/exit PnL)']);
    pnlRows.push([
      'Position Key',
      'Asset',
      'Market',
      'Outcome',
      'Buy Size',
      'Buy USDC',
      'Avg Entry',
      'Sell Size',
      'Sell USDC',
      'Avg Exit',
      'Realized PnL',
      'Unrealized PnL',
      'Total PnL',
      'Open Size',
      'Last Price (proxy)',
      'Last Trade ISO',
      'Status',
    ]);
    positionSummary.forEach((entry) => {
      pnlRows.push([
        entry.key,
        entry.asset,
        entry.market,
        entry.outcome,
        formatSheetNumber(entry.buySize),
        formatSheetNumber(entry.buyCost),
        formatSheetNumber(entry.avgEntry),
        formatSheetNumber(entry.sellSize),
        formatSheetNumber(entry.sellProceeds),
        formatSheetNumber(entry.avgExit),
        formatSheetNumber(entry.realizedPnl),
        formatSheetNumber(entry.unrealizedPnl),
        formatSheetNumber(entry.totalPnl),
        formatSheetNumber(entry.openSize),
        formatSheetNumber(entry.lastPrice),
        entry.lastTradeIso ?? formatTimestampIso(entry.lastTradeAt ?? undefined),
        entry.status,
      ]);
    });
  }

  if (positionHistory && positionHistory.length > 0) {
    if (pnlRows.length > 0) {
      pnlRows.push([]);
    }
    pnlRows.push(['Position History (computed from trade feed)']);
    pnlRows.push([
      'Position Key',
      'Asset',
      'Market',
      'Outcome',
      'Side',
      'Trade Price',
      'Trade Size',
      'Trade USDC',
      'Timestamp',
      'Timestamp ISO',
      'Position Size',
      'Position Cost Basis USDC',
      'Position Avg Price',
      'Realized PnL (Trade)',
      'Cumulative Realized PnL',
      'Position Status',
    ]);
    positionHistory.forEach((entry) => {
      pnlRows.push([
        entry.key,
        entry.asset,
        entry.market,
        entry.outcome,
        entry.side,
        formatSheetNumber(entry.tradePrice),
        formatSheetNumber(entry.tradeSize),
        formatSheetNumber(entry.tradeUsdc),
        entry.timestamp,
        entry.timestampIso ?? formatTimestampIso(entry.timestamp),
        formatSheetNumber(entry.positionSize),
        formatSheetNumber(entry.positionCostBasis),
        formatSheetNumber(entry.positionAvgPrice),
        formatSheetNumber(entry.realizedPnl),
        formatSheetNumber(entry.cumulativeRealizedPnl),
        entry.status,
      ]);
    });
  }

  if (lifetimeSummary?.daily && lifetimeSummary.daily.length > 0) {
    if (pnlRows.length > 0) {
      pnlRows.push([]);
    }
    pnlRows.push(['Daily PnL']);
    pnlRows.push(['Date', 'Net', 'Profit', 'Loss', 'Events']);
    lifetimeSummary.daily.forEach((day) => {
      pnlRows.push([day.date, day.net, day.profit, day.loss, day.events]);
    });
  }

  const pnlSheet = XLSX.utils.aoa_to_sheet(pnlRows.length > 0 ? pnlRows : [['No PnL data']]);
  XLSX.utils.book_append_sheet(workbook, pnlSheet, 'PNL');

  return workbook;
};

const buildRangeSummarySheet = (
  wallet: string,
  startInput: string,
  endInput: string,
  totalTrades: number,
  positionSummary: PositionSummaryEntry[],
  tradeTotals: ReturnType<typeof computeTradeTotals>,
  chunkIndex: number,
  chunkCount: number
) => {
  const totals = positionSummary.reduce(
    (acc, entry) => {
      const realized = Number.isFinite(entry.realizedPnl) ? entry.realizedPnl : 0;
      const unrealized = Number.isFinite(entry.unrealizedPnl ?? 0)
        ? (entry.unrealizedPnl ?? 0)
        : 0;
      const total = Number.isFinite(entry.totalPnl) ? entry.totalPnl : realized + unrealized;
      acc.realized += realized;
      acc.unrealized += unrealized;
      acc.total += total;
      if (total >= 0) {
        acc.profit += total;
      } else {
        acc.loss += total;
      }
      return acc;
    },
    { realized: 0, unrealized: 0, total: 0, profit: 0, loss: 0 }
  );

  const rows: Array<Array<string | number>> = [['Metric', 'Value']];
  const pushRow = (label: string, value: string | number | null | undefined) => {
    rows.push([label, value ?? '']);
  };

  pushRow('Exported At', new Date().toISOString());
  pushRow('Wallet', wallet);
  pushRow('Range Start', startInput || '-');
  pushRow('Range End', endInput || '-');
  pushRow('Total Trades (range)', totalTrades);
  pushRow('Buy Trades', tradeTotals.buyCount);
  pushRow('Sell Trades', tradeTotals.sellCount);
  pushRow('Total Buy USDC', formatSheetNumber(tradeTotals.buyUsd));
  pushRow('Total Sell USDC', formatSheetNumber(tradeTotals.sellUsd));
  pushRow('Net USDC', formatSheetNumber(tradeTotals.netUsd));
  pushRow('Positions (range)', positionSummary.length);
  pushRow('Total Realized PnL', formatSheetNumber(totals.realized));
  pushRow('Total Unrealized PnL', formatSheetNumber(totals.unrealized));
  pushRow('Total PnL', formatSheetNumber(totals.total));
  pushRow('Total Profit', formatSheetNumber(totals.profit));
  pushRow('Total Loss', formatSheetNumber(totals.loss));
  pushRow('This File', `${chunkIndex + 1} of ${chunkCount}`);
  pushRow('Note', 'PNL sheet in each part reflects only that file.');

  return XLSX.utils.aoa_to_sheet(rows);
};

const buildMarketHistoryWorkbook = (
  seriesId: string,
  markets: MarketSeriesEntry[],
  trades: MarketTradeEntry[]
) => {
  const workbook = XLSX.utils.book_new();

  const marketsSheet = XLSX.utils.aoa_to_sheet([
    [
      'series_id',
      'market_id',
      'slug',
      'question',
      'condition_id',
      'timestamp',
      'datetime',
      'end_date',
      'outcomes',
      'clob_token_ids',
    ],
    ...markets.map((market) => [
      seriesId,
      market.id,
      market.slug ?? '',
      market.question,
      market.conditionId ?? '',
      market.timestamp ?? '',
      market.timestamp ? new Date(market.timestamp * 1000).toISOString() : '',
      market.endDate ?? '',
      (market.outcomes ?? []).join(' | '),
      (market.clobTokenIds ?? []).join(' | '),
    ]),
  ]);
  XLSX.utils.book_append_sheet(workbook, marketsSheet, 'Markets');

  const marketMap = new Map(
    markets.map((market) => [market.conditionId ?? '', market])
  );

  const tradesSheet = XLSX.utils.aoa_to_sheet([
    [
      'series_id',
      'condition_id',
      'market_slug',
      'market_question',
      'trade_id',
      'timestamp',
      'datetime',
      'owner',
      'side',
      'outcome',
      'outcome_index',
      'price',
      'size',
      'usdc_size',
      'transaction_hash',
    ],
    ...trades.map((trade) => {
      const market = marketMap.get(trade.market);
      return [
        seriesId,
        trade.market,
        market?.slug ?? '',
        market?.question ?? '',
        trade.id,
        trade.timestamp ?? '',
        trade.timestamp ? new Date(trade.timestamp * 1000).toISOString() : '',
        trade.owner ?? '',
        trade.side ?? '',
        trade.outcome ?? '',
        trade.outcomeIndex ?? '',
        trade.price ?? '',
        trade.size ?? '',
        trade.usdcSize ?? '',
        trade.transactionHash ?? '',
      ];
    }),
  ]);
  XLSX.utils.book_append_sheet(workbook, tradesSheet, 'Trades');

  return workbook;
};

const buildMarketSnapshotWorkbook = (
  slug: string,
  market: Market | null,
  seriesId: string | null,
  tokenOptions: PriceHistoryOption[],
  priceSeries: PriceHistorySeries[],
  trades: MarketTradeEntry[]
) => {
  const workbook = XLSX.utils.book_new();
  const resolvedTokens =
    tokenOptions.length > 0
      ? tokenOptions
      : market?.clobTokenIds?.map((tokenId, index) => ({
          tokenId,
          outcome: market.outcomes?.[index],
          outcomeIndex: index,
        })) ?? [];

  const summaryRows: Array<Array<string | number>> = [
    ['Exported At', new Date().toISOString()],
    ['Slug', slug],
    ['Series ID', seriesId ?? ''],
    ['Condition ID', market?.conditionId ?? ''],
    ['Question', market?.question ?? ''],
    ['Category', market?.category ?? ''],
    ['End Date', market?.endDate ?? ''],
    ['Tokens', resolvedTokens.map((token) => token.tokenId).join(' | ')],
    ['Trades', trades.length],
    [
      'Price History Points',
      priceSeries.reduce((total, entry) => total + entry.points.length, 0),
    ],
  ];
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(summaryRows),
    'Summary'
  );

  const marketRows: Array<Array<string | number>> = [
    [
      'id',
      'slug',
      'question',
      'category',
      'tags',
      'end_date',
      'volume',
      'open_interest',
      'liquidity',
      'description',
      'condition_id',
      'active',
      'closed',
      'outcomes',
      'outcome_prices',
      'clob_token_ids',
    ],
  ];
  if (market) {
    const volume = market.volume24hr ?? market.volume ?? '';
    marketRows.push([
      market.id,
      market.slug ?? slug,
      market.question,
      market.category ?? '',
      market.tags?.join(' | ') ?? '',
      market.endDate ?? '',
      volume,
      market.openInterest ?? '',
      market.liquidity ?? '',
      market.description ?? '',
      market.conditionId ?? '',
      market.active === undefined ? '' : market.active ? 'true' : 'false',
      market.closed === undefined ? '' : market.closed ? 'true' : 'false',
      market.outcomes?.join(' | ') ?? '',
      market.outcomePrices?.join(' | ') ?? '',
      market.clobTokenIds?.join(' | ') ?? '',
    ]);
  } else {
    marketRows.push(['', slug, '', '', '', '', '', '', '', '', '', '', '', '', '', '']);
  }
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(marketRows),
    'Market'
  );

  const tokenRows: Array<Array<string | number>> = [['token_id', 'outcome', 'outcome_index']];
  resolvedTokens.forEach((token) => {
    tokenRows.push([
      token.tokenId,
      token.outcome ?? '',
      token.outcomeIndex ?? '',
    ]);
  });
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(tokenRows),
    'Tokens'
  );

  const tradeRows: Array<Array<string | number>> = [
    [
      'slug',
      'condition_id',
      'trade_id',
      'timestamp',
      'datetime',
      'owner',
      'side',
      'outcome',
      'outcome_index',
      'price',
      'size',
      'usdc_size',
      'transaction_hash',
    ],
  ];
  trades.forEach((trade) => {
    tradeRows.push([
      slug,
      market?.conditionId ?? trade.market ?? '',
      trade.id,
      trade.timestamp ?? '',
      trade.timestamp ? new Date(trade.timestamp * 1000).toISOString() : '',
      trade.owner ?? '',
      trade.side ?? '',
      trade.outcome ?? '',
      trade.outcomeIndex ?? '',
      formatSheetNumber(trade.price ?? null),
      formatSheetNumber(trade.size ?? null),
      formatSheetNumber(trade.usdcSize ?? null),
      trade.transactionHash ?? '',
    ]);
  });
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(tradeRows),
    'Trades'
  );

  const historyRows: Array<Array<string | number>> = [
    [
      'tokenId',
      'outcome',
      'outcomeIndex',
      'timestamp',
      'datetime',
      'token_price',
      'price_to_beat',
      'current_price',
      'liquidity_clob',
    ],
  ];
  priceSeries.forEach((entry) => {
    const normalized = normalizePriceHistorySeriesMeta(entry);
    const priceToBeat = normalized.priceToBeat ?? '';
    const currentPrice = normalized.currentPrice ?? '';
    const liquidityClob = normalized.liquidityClob ?? '';
    normalized.points.forEach((point) => {
      historyRows.push([
        normalized.tokenId,
        normalized.outcome ?? '',
        normalized.outcomeIndex !== undefined ? String(normalized.outcomeIndex) : '',
        point.timestamp,
        new Date(point.timestamp * 1000).toISOString(),
        point.price,
        priceToBeat,
        currentPrice,
        liquidityClob,
      ]);
    });
  });
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(historyRows),
    'Price History'
  );

  return workbook;
};

const parseLiveStreamPayload = (raw: string) => {
  let parsed: unknown = raw;
  if (raw) {
    try {
      parsed = JSON.parse(raw);
    } catch (_error) {
      parsed = raw;
    }
  }
  const record =
    parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
  const payload =
    record && typeof record.data === 'object'
      ? (record.data as Record<string, unknown>)
      : record;
  const channel =
    (record?.channel as string | undefined) ??
    (payload?.channel as string | undefined) ??
    (record?.type as string | undefined) ??
    '';
  const tokenId = payload ? extractTokenIdFromPayload(payload) : '';
  const price = payload ? extractPriceFromPayload(payload) : null;
  const side =
    payload && typeof payload.side === 'string'
      ? payload.side
      : payload && typeof payload.s === 'string'
        ? payload.s
        : '';
  const bookStats = payload ? extractBookStatsFromPayload(payload) : null;
  const bidsRaw = payload?.bids ?? payload?.bid ?? payload?.b ?? null;
  const asksRaw = payload?.asks ?? payload?.ask ?? payload?.a ?? null;

  return {
    channel,
    tokenId,
    price,
    side,
    bestBid: bookStats?.bestBid ?? null,
    bestAsk: bookStats?.bestAsk ?? null,
    mid: bookStats?.mid ?? null,
    spread: bookStats?.spread ?? null,
    bidsRaw,
    asksRaw,
  };
};

const parseBookLevelsForExport = (raw: unknown) => {
  if (!Array.isArray(raw)) {
    return [] as Array<{ price: number; size: number }>;
  }
  const levels: Array<{ price: number; size: number }> = [];
  raw.forEach((entry) => {
    let price: number | null = null;
    let size: number | null = null;
    if (Array.isArray(entry) && entry.length >= 2) {
      price = parseFiniteNumber(entry[0]);
      size = parseFiniteNumber(entry[1]);
    } else if (entry && typeof entry === 'object') {
      const record = entry as Record<string, unknown>;
      price = parseFiniteNumber(record.price ?? record.p ?? record[0]);
      size = parseFiniteNumber(record.size ?? record.s ?? record[1]);
    }
    if (price !== null && size !== null) {
      levels.push({ price, size });
    }
  });
  return levels;
};

const buildLiveStreamWorkbook = (
  slug: string,
  tokens: string[],
  messages: LiveStreamMessage[],
  meta?: { timestamp: number; intervalSeconds: number },
  priceSnapshot?: { priceToBeat: number | null; currentPrice: number | null },
  snapshots: LiveStreamPriceSnapshotEntry[] = []
) => {
  const workbook = XLSX.utils.book_new();
  const intervalEnd =
    meta && Number.isFinite(meta.timestamp)
      ? meta.timestamp + meta.intervalSeconds
      : null;
  const summaryRows: Array<Array<string | number>> = [
    ['Exported At', new Date().toISOString()],
    ['Slug', slug],
    ['Tokens', tokens.join(' | ')],
    ['Message Count', messages.length],
    ['Price To Beat', priceSnapshot?.priceToBeat ?? ''],
    ['Current Price', priceSnapshot?.currentPrice ?? ''],
    ['Start Timestamp', meta?.timestamp ?? ''],
    [
      'Start Datetime',
      meta?.timestamp ? new Date(meta.timestamp * 1000).toISOString() : '',
    ],
    ['End Timestamp', intervalEnd ?? ''],
    [
      'End Datetime',
      intervalEnd ? new Date(intervalEnd * 1000).toISOString() : '',
    ],
  ];
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(summaryRows),
    'Summary'
  );

  const sortedMessages = [...messages].sort((a, b) => a.timestamp - b.timestamp);
  const streamRows: Array<Array<string | number>> = [
    [
      'slug',
      'timestamp_ms',
      'timestamp_sec',
      'datetime',
      'channel',
      'token_id',
      'price',
      'side',
      'best_bid',
      'best_ask',
      'mid',
      'spread',
      'summary',
    ],
  ];
  const bookRows: Array<Array<string | number>> = [
    [
      'slug',
      'timestamp_ms',
      'timestamp_sec',
      'datetime',
      'token_id',
      'side',
      'level_index',
      'price',
      'size',
    ],
  ];
  sortedMessages.forEach((message) => {
    const parsed = parseLiveStreamPayload(message.rawFull ?? message.raw ?? '');
    const timestampMs = String(message.timestamp);
    const timestampSec = Math.floor(message.timestamp / 1000);
    streamRows.push([
      slug,
      timestampMs,
      timestampSec,
      new Date(message.timestamp).toISOString(),
      parsed.channel || message.channel || '',
      parsed.tokenId || message.tokenId || '',
      parsed.price ?? '',
      parsed.side ?? '',
      parsed.bestBid ?? '',
      parsed.bestAsk ?? '',
      parsed.mid ?? '',
      parsed.spread ?? '',
      message.summary ?? '',
    ]);

    const bids = parseBookLevelsForExport(parsed.bidsRaw);
    const asks = parseBookLevelsForExport(parsed.asksRaw);
    bids.forEach((level, index) => {
      bookRows.push([
        slug,
        timestampMs,
        timestampSec,
        new Date(message.timestamp).toISOString(),
        parsed.tokenId || message.tokenId || '',
        'bid',
        index + 1,
        level.price,
        level.size,
      ]);
    });
    asks.forEach((level, index) => {
      bookRows.push([
        slug,
        timestampMs,
        timestampSec,
        new Date(message.timestamp).toISOString(),
        parsed.tokenId || message.tokenId || '',
        'ask',
        index + 1,
        level.price,
        level.size,
      ]);
    });
  });
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(streamRows),
    'Stream'
  );
  if (bookRows.length > 1) {
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.aoa_to_sheet(bookRows),
      'Order Book'
    );
  }
  if (snapshots.length > 0) {
    const snapshotRows: Array<Array<string | number>> = [
      ['slug', 'timestamp_ms', 'timestamp_sec', 'datetime', 'price_to_beat', 'current_price'],
    ];
    const sortedSnapshots = [...snapshots].sort(
      (a, b) => a.timestampMs - b.timestampMs
    );
    sortedSnapshots.forEach((entry) => {
      snapshotRows.push([
        slug,
        String(entry.timestampMs),
        Math.floor(entry.timestampMs / 1000),
        new Date(entry.timestampMs).toISOString(),
        entry.priceToBeat ?? '',
        entry.currentPrice ?? '',
      ]);
    });
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.aoa_to_sheet(snapshotRows),
      'Price Snapshots'
    );
  }

  return workbook;
};

const downloadWorkbook = (workbook: XLSX.WorkBook, fileName: string) => {
  const data = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
  const blob = new Blob([data], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
};

const authFetch = async (token: string | null, url: string, options?: RequestInit) => {
  const headers = new Headers(options?.headers || {});
  if (token) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  return fetch(url, { ...options, headers });
};

const isValidAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);

const extractPriceHistoryPoints = (payload: unknown): PriceHistoryPoint[] => {
  let raw = payload as unknown;
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
    const source = raw as Record<string, unknown>;
    raw =
      source.prices ||
      source.history ||
      source.data ||
      source.priceHistory ||
      source.price_history ||
      raw;
  }
  if (!Array.isArray(raw)) {
    return [];
  }
  const points: PriceHistoryPoint[] = [];
  raw.forEach((item) => {
    let timestamp: number | null = null;
    let price: number | null = null;

    if (Array.isArray(item)) {
      if (item.length >= 2) {
        timestamp = parseNumber(item[0]);
        price = parseNumber(item[1]);
      }
    } else if (item && typeof item === 'object') {
      const record = item as Record<string, unknown>;
      const tsCandidate =
        record.timestamp ?? record.ts ?? record.t ?? record.time ?? (record as { 0?: unknown })[0];
      const priceCandidate =
        record.price ?? record.p ?? record.value ?? (record as { 1?: unknown })[1];
      timestamp = parseNumber(tsCandidate as number | string | undefined);
      price = parseNumber(priceCandidate as number | string | undefined);
    }

    if (timestamp === null || price === null) {
      return;
    }
    if (!Number.isFinite(timestamp) || !Number.isFinite(price)) {
      return;
    }
    if (timestamp <= 0) {
      return;
    }
    points.push({ timestamp: normalizeTimestampSeconds(timestamp), price });
  });

  return points.sort((a, b) => a.timestamp - b.timestamp);
};

const normalizePriceHistorySeriesMeta = (entry: PriceHistorySeries): PriceHistorySeries => {
  if (entry.points.length === 0) {
    return { ...entry };
  }
  const first = entry.points[0];
  const last = entry.points[entry.points.length - 1];
  return {
    ...entry,
    priceToBeat: entry.priceToBeat ?? first.price,
    currentPrice: entry.currentPrice ?? last.price,
  };
};

const downsamplePriceHistory = (points: PriceHistoryPoint[], maxPoints: number) => {
  if (maxPoints <= 0 || points.length <= maxPoints) {
    return points;
  }
  if (maxPoints === 1) {
    return points.slice(-1);
  }
  const total = points.length;
  const step = (total - 1) / (maxPoints - 1);
  const sampled: PriceHistoryPoint[] = [];
  for (let i = 0; i < maxPoints; i += 1) {
    const index = Math.round(i * step);
    const entry = points[index];
    if (entry) {
      sampled.push(entry);
    }
  }
  return sampled;
};

const limitPriceHistoryPoints = (points: PriceHistoryPoint[], maxRows: number | null) => {
  if (!maxRows || maxRows <= 0 || points.length <= maxRows) {
    return points;
  }
  return points.slice(-maxRows);
};

const buildPriceHistoryChart = (points: PriceHistoryPoint[]) => {
  if (points.length < 2) {
    return null;
  }
  const width = 240;
  const height = 90;
  const padding = 6;
  const minPrice = Math.min(...points.map((point) => point.price));
  const maxPrice = Math.max(...points.map((point) => point.price));
  const priceRange = maxPrice - minPrice || 1;
  const start = points[0].timestamp;
  const end = points[points.length - 1].timestamp;
  const timeRange = end - start || 1;

  const coords = points
    .map((point) => {
      const x =
        padding + ((point.timestamp - start) / timeRange) * (width - padding * 2);
      const y =
        padding +
        (1 - (point.price - minPrice) / priceRange) * (height - padding * 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');

  return {
    width,
    height,
    points: coords,
    minPrice,
    maxPrice,
  };
};

const buildPriceHistoryCsv = (entry: PriceHistorySeries) => {
  const normalized = normalizePriceHistorySeriesMeta(entry);
  const rows: Array<Array<string | number>> = [
    ['timestamp', 'datetime', 'token_price', 'price_to_beat', 'current_price', 'liquidity_clob'],
  ];
  const priceToBeat = normalized.priceToBeat ?? '';
  const currentPrice = normalized.currentPrice ?? '';
  const liquidityClob = normalized.liquidityClob ?? '';
  normalized.points.forEach((point) => {
    rows.push([
      point.timestamp,
      new Date(point.timestamp * 1000).toISOString(),
      point.price,
      priceToBeat,
      currentPrice,
      liquidityClob,
    ]);
  });
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildPriceHistorySeriesCsv = (series: PriceHistorySeries[]) => {
  if (series.length <= 1) {
    return buildPriceHistoryCsv(
      series[0] ?? {
        tokenId: 'market',
        points: [],
      }
    );
  }
  const rows: Array<Array<string | number>> = [
    [
      'tokenId',
      'outcome',
      'outcomeIndex',
      'timestamp',
      'datetime',
      'token_price',
      'price_to_beat',
      'current_price',
      'liquidity_clob',
    ],
  ];
  series.forEach((entry) => {
    const normalized = normalizePriceHistorySeriesMeta(entry);
    const priceToBeat = normalized.priceToBeat ?? '';
    const currentPrice = normalized.currentPrice ?? '';
    const liquidityClob = normalized.liquidityClob ?? '';
    normalized.points.forEach((point) => {
      rows.push([
        normalized.tokenId,
        normalized.outcome ?? '',
        normalized.outcomeIndex !== undefined ? String(normalized.outcomeIndex) : '',
        point.timestamp,
        new Date(point.timestamp * 1000).toISOString(),
        point.price,
        priceToBeat,
        currentPrice,
        liquidityClob,
      ]);
    });
  });
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const downloadCsv = (csv: string, fileName: string) => {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
};

const readJsonResponse = async <T,>(response: Response): Promise<T> => {
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return (await response.json()) as T;
  }
  const text = await response.text();
  const normalized = text.replace(/\s+/g, ' ').trim();
  const snippet = normalized.slice(0, 160);
  const lower = normalized.toLowerCase();
  const hint =
    lower.includes('<!doctype') || lower.includes('<html')
      ? 'Unexpected HTML response. Is the API server running?'
      : 'Unexpected response body.';
  const suffix = snippet ? ` ${snippet}` : '';
  throw new Error(`${hint} (status ${response.status}).${suffix}`);
};

const parseTokenIdInput = (value: string) => {
  const tokens = value
    .split(/[\s,&;]+/g)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0 && /^\d+$/.test(entry));
  return Array.from(new Set(tokens));
};

const extractTokenIdFromPayload = (payload: Record<string, unknown>) => {
  const token =
    payload.tokenId ??
    payload.token_id ??
    payload.token ??
    payload.market ??
    payload.conditionId ??
    payload.condition_id ??
    payload.asset;
  if (token === undefined || token === null) {
    return undefined;
  }
  return String(token);
};

const extractBookSidePrices = (entries: unknown) => {
  if (!Array.isArray(entries)) {
    return [];
  }
  const prices: number[] = [];
  entries.forEach((entry) => {
    let price: number | null = null;
    if (Array.isArray(entry)) {
      price = parseFiniteNumber(entry[0]);
    } else if (entry && typeof entry === 'object') {
      const record = entry as Record<string, unknown>;
      price = parseFiniteNumber(
        record.price ?? record.p ?? (record as { 0?: unknown })[0]
      );
    }
    if (price !== null) {
      prices.push(price);
    }
  });
  return prices;
};

const extractBookStatsFromPayload = (payload: Record<string, unknown>) => {
  const bids = payload.bids ?? payload.bid ?? payload.b;
  const asks = payload.asks ?? payload.ask ?? payload.a;
  if (!bids && !asks) {
    return null;
  }
  const tokenId = extractTokenIdFromPayload(payload);
  if (!tokenId) {
    return null;
  }
  const bidPrices = extractBookSidePrices(bids);
  const askPrices = extractBookSidePrices(asks);
  if (bidPrices.length === 0 && askPrices.length === 0) {
    return null;
  }
  const bestBid = bidPrices.length > 0 ? Math.max(...bidPrices) : undefined;
  const bestAsk = askPrices.length > 0 ? Math.min(...askPrices) : undefined;
  const mid =
    bestBid !== undefined && bestAsk !== undefined ? (bestBid + bestAsk) / 2 : undefined;
  const spread =
    bestBid !== undefined && bestAsk !== undefined ? bestAsk - bestBid : undefined;
  return {
    tokenId,
    bestBid,
    bestAsk,
    mid,
    spread,
    updatedAt: Date.now(),
  };
};

const extractPriceFromPayload = (payload: Record<string, unknown>) => {
  const priceCandidate =
    payload.price ??
    payload.last_price ??
    payload.lastPrice ??
    payload.p ??
    payload.value ??
    payload.last ??
    null;
  return parseFiniteNumber(priceCandidate);
};

const buildLiveStreamMessage = (raw: string) => {
  let parsed: unknown = raw;
  if (raw) {
    try {
      parsed = JSON.parse(raw);
    } catch (_error) {
      parsed = raw;
    }
  }
  const record =
    parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
  const data =
    record && typeof record.data === 'object'
      ? (record.data as Record<string, unknown>)
      : record;
  const channel =
    (record?.channel as string | undefined) ??
    (data?.channel as string | undefined) ??
    (record?.type as string | undefined);
  const tokenId = data ? extractTokenIdFromPayload(data) : undefined;
  const bookStats = data ? extractBookStatsFromPayload(data) : null;
  const price = data ? extractPriceFromPayload(data) : null;

  let summary = 'update';
  if (bookStats) {
    summary = `book ${bookStats.tokenId} bid ${formatTokenPrice(
      bookStats.bestBid ?? null
    )} ask ${formatTokenPrice(bookStats.bestAsk ?? null)}`;
  } else if (price !== null) {
    summary = `${tokenId ? `${tokenId} ` : ''}price ${formatTokenPrice(price)}`;
  } else if (channel) {
    summary = `${channel} update`;
  }

  const rawText =
    typeof parsed === 'string'
      ? parsed
      : parsed
        ? JSON.stringify(parsed)
        : 'message';
  const rawPreview = rawText.length > 240 ? `${rawText.slice(0, 237)}...` : rawText;

  return {
    message: {
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      timestamp: Date.now(),
      channel,
      tokenId: bookStats?.tokenId ?? tokenId,
      price,
      summary,
      raw: rawPreview,
      rawFull: rawText,
    },
    bookStats,
  };
};

const parseOutcomeLabels = (value: string) => {
  if (!value.trim()) {
    return [];
  }
  return value
    .split(/[|,]+/g)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
};

const inferTokenLabels = (
  labels: string[],
  count: number,
  marketHint: string,
  prefixHint: string
) => {
  if (labels.length >= count) {
    return labels.slice(0, count);
  }
  const hint = `${marketHint} ${prefixHint}`.toLowerCase();
  if (count === 2 && /(up[-\s]?down|up[-\s]?or[-\s]?down)/.test(hint)) {
    return ['Up', 'Down'];
  }
  return Array.from({ length: count }, (_, index) => `Token ${index + 1}`);
};

const formatTokenLabel = (tokenId: string) =>
  `token-${tokenId.slice(0, 6)}${tokenId.slice(-4)}`;

const buildSparklinePath = (points: number[], width: number, height: number) => {
  if (!points || points.length < 2) {
    return '';
  }
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = max - min || 1;
  const step = width / (points.length - 1);
  return points
    .map((value, index) => {
      const x = index * step;
      const y = height - ((value - min) / range) * height;
      return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');
};

const buildActivityCsv = (trades: TradeActivity[]) => {
  const rows: Array<Array<unknown>> = [
    [
      'id',
      'userAddress',
      'title',
      'slug',
      'eventSlug',
      'side',
      'outcome',
      'outcomeIndex',
      'price',
      'size',
      'usdcSize',
      'timestamp',
      'timestampIso',
      'transactionHash',
    ],
  ];

  trades.forEach((trade) => {
    rows.push([
      trade.id,
      trade.userAddress,
      trade.title ?? '',
      trade.slug ?? '',
      trade.eventSlug ?? '',
      trade.side ?? '',
      trade.outcome ?? '',
      trade.outcomeIndex ?? '',
      trade.price ?? '',
      trade.size ?? '',
      trade.usdcSize ?? '',
      trade.timestamp ?? '',
      formatTimestampIso(trade.timestamp),
      trade.transactionHash ?? '',
    ]);
  });

  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildTraderSummaryCsv = (
  wallet: string,
  summary: TraderProfileSummary,
  range?: TraderProfileResponse['range']
) => {
  const rows: Array<Array<unknown>> = [['metric', 'value']];
  rows.push(['exported_at', new Date().toISOString()]);
  rows.push(['wallet', wallet]);
  rows.push(['range_start_ts', range?.startTs ?? '']);
  rows.push(['range_start_iso', range?.startTs ? formatTimestampIso(range.startTs) : '']);
  rows.push(['range_end_ts', range?.endTs ?? '']);
  rows.push(['range_end_iso', range?.endTs ? formatTimestampIso(range.endTs) : '']);
  rows.push(['range_limit', range?.limit ?? '']);
  rows.push(['total_trades', summary.totalTrades]);
  rows.push(['wins', summary.wins]);
  rows.push(['losses', summary.losses]);
  rows.push(['win_rate', summary.winRate ?? '']);
  rows.push([
    'win_rate_pct',
    summary.winRate !== null && summary.winRate !== undefined
      ? (summary.winRate * 100).toFixed(2)
      : '',
  ]);
  rows.push(['realized_pnl', summary.realizedPnl]);
  rows.push(['unrealized_pnl', summary.unrealizedPnl]);
  rows.push(['total_pnl', summary.totalPnl]);
  rows.push(['open_positions', summary.openPositions]);
  rows.push(['closed_positions', summary.closedPositions]);
  rows.push(['balance', summary.balance ?? '']);
  rows.push(['positions_value', summary.positionsValue ?? '']);
  rows.push(['portfolio_value', summary.portfolioValue ?? '']);
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildTraderPositionsCsv = (positions: PositionSummaryEntry[]) => {
  const rows: Array<Array<unknown>> = [
    [
      'key',
      'asset',
      'market',
      'outcome',
      'buy_size',
      'buy_cost',
      'avg_entry',
      'sell_size',
      'sell_proceeds',
      'avg_exit',
      'realized_pnl',
      'unrealized_pnl',
      'total_pnl',
      'open_size',
      'last_price',
      'last_trade_at',
      'last_trade_iso',
      'status',
    ],
  ];

  positions.forEach((entry) => {
    rows.push([
      entry.key,
      entry.asset,
      entry.market,
      entry.outcome,
      entry.buySize,
      entry.buyCost,
      entry.avgEntry,
      entry.sellSize,
      entry.sellProceeds,
      entry.avgExit,
      entry.realizedPnl,
      entry.unrealizedPnl ?? '',
      entry.totalPnl,
      entry.openSize,
      entry.lastPrice ?? '',
      entry.lastTradeAt ?? '',
      entry.lastTradeIso ?? (entry.lastTradeAt ? formatTimestampIso(entry.lastTradeAt) : ''),
      entry.status,
    ]);
  });

  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildTraderPositionHistoryCsv = (history: PositionHistoryEntry[]) => {
  const rows: Array<Array<unknown>> = [
    [
      'key',
      'asset',
      'market',
      'outcome',
      'side',
      'trade_price',
      'trade_size',
      'trade_usdc',
      'timestamp',
      'timestamp_iso',
      'position_size',
      'position_cost_basis',
      'position_avg_price',
      'realized_pnl',
      'cumulative_realized_pnl',
      'status',
    ],
  ];

  history.forEach((entry) => {
    rows.push([
      entry.key,
      entry.asset,
      entry.market,
      entry.outcome,
      entry.side,
      entry.tradePrice,
      entry.tradeSize,
      entry.tradeUsdc,
      entry.timestamp,
      entry.timestampIso ?? formatTimestampIso(entry.timestamp),
      entry.positionSize,
      entry.positionCostBasis,
      entry.positionAvgPrice,
      entry.realizedPnl,
      entry.cumulativeRealizedPnl,
      entry.status,
    ]);
  });

  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildTraderTradesCsv = (trades: TradeActivity[]) => {
  const rows: Array<Array<unknown>> = [
    [
      'id',
      'userAddress',
      'title',
      'slug',
      'eventSlug',
      'side',
      'outcome',
      'outcomeIndex',
      'price',
      'size',
      'usdcSize',
      'timestamp',
      'timestampIso',
      'transactionHash',
      'category',
      'tags',
    ],
  ];

  trades.forEach((trade) => {
    rows.push([
      trade.id,
      trade.userAddress,
      trade.title ?? '',
      trade.slug ?? '',
      trade.eventSlug ?? '',
      trade.side ?? '',
      trade.outcome ?? '',
      trade.outcomeIndex ?? '',
      trade.price ?? '',
      trade.size ?? '',
      trade.usdcSize ?? '',
      trade.timestamp ?? '',
      formatTimestampIso(trade.timestamp),
      trade.transactionHash ?? '',
      trade.category ?? '',
      trade.tags?.join('|') ?? '',
    ]);
  });

  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildCategorySummaryCsv = (categories: CategorySummaryEntry[]) => {
  const rows: Array<Array<unknown>> = [
    [
      'category',
      'realized_pnl',
      'unrealized_pnl',
      'total_pnl',
      'trade_count',
      'win_count',
      'loss_count',
    ],
  ];
  categories.forEach((entry) => {
    rows.push([
      entry.category,
      entry.realizedPnl,
      entry.unrealizedPnl,
      entry.totalPnl,
      entry.tradeCount,
      entry.winCount,
      entry.lossCount,
    ]);
  });
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildDailyPnlCsv = (daily: DailyTradePnlEntry[]) => {
  const rows: Array<Array<unknown>> = [['date', 'net', 'profit', 'loss', 'trades']];
  daily.forEach((entry) => {
    rows.push([entry.date, entry.net, entry.profit, entry.loss, entry.trades]);
  });
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildTraderScanCsv = (traders: TraderScanEntry[]) => {
  const rows: Array<Array<unknown>> = [
    ['address', 'trade_count', 'volume', 'last_trade_at', 'last_trade_iso'],
  ];
  traders.forEach((entry) => {
    rows.push([
      entry.address,
      entry.tradeCount,
      entry.volume,
      entry.lastTradeAt ?? '',
      entry.lastTradeAt ? formatTimestampIso(entry.lastTradeAt) : '',
    ]);
  });
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

const buildMarketSearchCsv = (marketsList: Market[]) => {
  const rows: Array<Array<unknown>> = [
    [
      'id',
      'question',
      'slug',
      'category',
      'tags',
      'volume',
      'open_interest',
      'liquidity',
      'end_date',
      'description',
      'condition_id',
      'active',
      'closed',
      'outcomes',
      'outcome_prices',
      'clob_token_ids',
      'source',
    ],
  ];
  marketsList.forEach((market) => {
    const volume = market.volume24hr ?? market.volume ?? '';
    rows.push([
      market.id,
      market.question,
      market.slug ?? '',
      market.category ?? '',
      market.tags?.join('|') ?? '',
      volume,
      market.openInterest ?? '',
      market.liquidity ?? '',
      market.endDate ?? '',
      market.description ?? '',
      market.conditionId ?? '',
      market.active ?? '',
      market.closed ?? '',
      market.outcomes?.join('|') ?? '',
      market.outcomePrices?.join('|') ?? '',
      market.clobTokenIds?.join('|') ?? '',
      market.source ?? '',
    ]);
  });
  return rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
};

export default function App() {
  const [manualTrades, setManualTrades] = useState<TradeActivity[]>([]);
  const [monitorInput, setMonitorInput] = useState('');
  const [manualError, setManualError] = useState<string | null>(null);
  const [exportLoading, setExportLoading] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const [exportWarning, setExportWarning] = useState<string | null>(null);
  const [markets, setMarkets] = useState<Market[]>([]);
  const [leaders, setLeaders] = useState<LeaderEntry[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedMarketId, setSelectedMarketId] = useState<number | null>(null);
  const [authToken, setAuthToken] = useState<string | null>(null);
  const [authUser, setAuthUser] = useState<AuthUser | null>(null);
  const [authMode, setAuthMode] = useState<'login' | 'signup'>('login');
  const [authEmail, setAuthEmail] = useState('');
  const [authPassword, setAuthPassword] = useState('');
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [walletEntry, setWalletEntry] = useState('');
  const [walletSaveLoading, setWalletSaveLoading] = useState(false);
  const [walletSaveError, setWalletSaveError] = useState<string | null>(null);
  const [adminTab, setAdminTab] = useState<'users' | 'activity'>('users');
  const [adminUsers, setAdminUsers] = useState<AuthUser[]>([]);
  const [adminActivity, setAdminActivity] = useState<AdminActivity[]>([]);
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminError, setAdminError] = useState<string | null>(null);
  const [theme, setTheme] = useState<'dark' | 'light'>('dark');
  const [activePage, setActivePage] = useState<'overview' | 'trader' | 'market'>(
    'overview'
  );
  const [walletSummary, setWalletSummary] = useState<WalletSummary | null>(null);
  const [walletSummaryError, setWalletSummaryError] = useState<string | null>(null);
  const [walletSummaryLoading, setWalletSummaryLoading] = useState(false);
  const [lifetimeSummary, setLifetimeSummary] = useState<LifetimeSummary | null>(null);
  const [lifetimeSummaryError, setLifetimeSummaryError] = useState<string | null>(null);
  const [lifetimeSummaryLoading, setLifetimeSummaryLoading] = useState(false);
  const [priceHistoryMarket, setPriceHistoryMarket] = useState('');
  const [priceHistoryPrefix, setPriceHistoryPrefix] = useState('');
  const [priceHistoryOutcome, setPriceHistoryOutcome] = useState('');
  const [priceHistoryOutcomeIndex, setPriceHistoryOutcomeIndex] = useState('');
  const [priceHistoryTokenIds, setPriceHistoryTokenIds] = useState(
    '38245269374392953810937710143442830800902605870225484824902313472906481929660, 35924729975423057898889838520095084932060464944338061797004423414969443450049'
  );
  const [priceHistoryTokenLabels, setPriceHistoryTokenLabels] = useState('');
  const [priceHistoryStart, setPriceHistoryStart] = useState('');
  const [priceHistoryEnd, setPriceHistoryEnd] = useState('');
  const [priceHistoryFidelity, setPriceHistoryFidelity] = useState('');
  const [priceHistoryRowLimit, setPriceHistoryRowLimit] = useState('');
  const [priceHistoryPoints, setPriceHistoryPoints] = useState<PriceHistoryPoint[]>([]);
  const [priceHistoryMeta, setPriceHistoryMeta] = useState<PriceHistoryMeta | null>(null);
  const [priceHistorySeries, setPriceHistorySeries] = useState<PriceHistorySeries[]>([]);
  const [priceHistoryActiveToken, setPriceHistoryActiveToken] = useState<string | null>(null);
  const [priceHistoryOptions, setPriceHistoryOptions] = useState<PriceHistoryOption[]>([]);
  const [priceHistoryLoading, setPriceHistoryLoading] = useState(false);
  const [priceHistoryError, setPriceHistoryError] = useState<string | null>(null);
  const [priceHistoryTokenLookupLoading, setPriceHistoryTokenLookupLoading] =
    useState(false);
  const [priceHistoryTokenLookupError, setPriceHistoryTokenLookupError] = useState<
    string | null
  >(null);

  const applyPriceHistorySeries = (
    series: PriceHistorySeries[],
    preferredToken?: string | null
  ) => {
    const normalizedSeries = series.map(normalizePriceHistorySeriesMeta);
    const active =
      normalizedSeries.find((entry) => entry.tokenId === preferredToken) ??
      normalizedSeries[0] ??
      null;
    setPriceHistorySeries(normalizedSeries);
    setPriceHistoryActiveToken(active?.tokenId ?? null);
    setPriceHistoryPoints(active?.points ?? []);
    setPriceHistoryMeta(
      active
        ? {
            tokenId: active.tokenId,
            slug: active.slug,
            outcome: active.outcome,
            outcomeIndex: active.outcomeIndex,
            priceToBeat: active.priceToBeat ?? null,
            currentPrice: active.currentPrice ?? null,
            liquidityClob: active.liquidityClob ?? null,
          }
        : null
    );
  };
  const [resolvePrefix, setResolvePrefix] = useState('');
  const [resolveStart, setResolveStart] = useState('');
  const [resolveEnd, setResolveEnd] = useState('');
  const [resolveLimit, setResolveLimit] = useState('');
  const [resolveResults, setResolveResults] = useState<ResolvedMarket[]>([]);
  const [resolveLoading, setResolveLoading] = useState(false);
  const [resolveError, setResolveError] = useState<string | null>(null);
  const [resolveWarning, setResolveWarning] = useState<string | null>(null);
  const [resolveDownloadLoading, setResolveDownloadLoading] = useState(false);
  const [rangeExportStart, setRangeExportStart] = useState('');
  const [rangeExportEnd, setRangeExportEnd] = useState('');
  const [rangeExportLimit, setRangeExportLimit] = useState('5000');
  const [rangeExportLoading, setRangeExportLoading] = useState(false);
  const [rangeExportError, setRangeExportError] = useState<string | null>(null);
  const [rangeExportWarning, setRangeExportWarning] = useState<string | null>(null);
  const [rangeExportEarliestLoading, setRangeExportEarliestLoading] = useState(false);
  const [rangeExportEarliestError, setRangeExportEarliestError] = useState<string | null>(
    null
  );
  const [rangeExportProgress, setRangeExportProgress] = useState(0);
  const [rangeExportPages, setRangeExportPages] = useState(0);
  const [rangeExportCancelRequested, setRangeExportCancelRequested] = useState(false);
  const [rangeExportResumeCursor, setRangeExportResumeCursor] = useState<number | null>(
    null
  );
  const [rangeExportResumeKeys, setRangeExportResumeKeys] = useState<string[]>([]);
  const [rangeExportPreview, setRangeExportPreview] = useState<TradeActivity[]>([]);
  const [rangeExportPreviewTotal, setRangeExportPreviewTotal] = useState<number | null>(
    null
  );
  const [rangeExportAutoFilled, setRangeExportAutoFilled] = useState(false);
  const [positionsWalletInput, setPositionsWalletInput] = useState('');
  const [positionsTab, setPositionsTab] = useState<'active' | 'closed'>('active');
  const [positionsPageSize, setPositionsPageSize] = useState('25');
  const [positionsLoading, setPositionsLoading] = useState(false);
  const [positionsError, setPositionsError] = useState<string | null>(null);
  const [activePositions, setActivePositions] = useState<PositionItem[]>([]);
  const [activePositionsCount, setActivePositionsCount] = useState<number | null>(null);
  const [activePositionsOffset, setActivePositionsOffset] = useState(0);
  const [closedPositions, setClosedPositions] = useState<PositionItem[]>([]);
  const [closedPositionsCount, setClosedPositionsCount] = useState<number | null>(null);
  const [closedPositionsOffset, setClosedPositionsOffset] = useState(0);
  const [traderProfileWallet, setTraderProfileWallet] = useState('');
  const [traderProfileStart, setTraderProfileStart] = useState('');
  const [traderProfileEnd, setTraderProfileEnd] = useState('');
  const [traderProfileLimit, setTraderProfileLimit] = useState('5000');
  const [traderProfileLoading, setTraderProfileLoading] = useState(false);
  const [traderProfileError, setTraderProfileError] = useState<string | null>(null);
  const [traderProfileWarning, setTraderProfileWarning] = useState<string | null>(null);
  const [traderScanLimit, setTraderScanLimit] = useState('20');
  const [traderScanMarketLimit, setTraderScanMarketLimit] = useState('25');
  const [traderScanTradeLimit, setTraderScanTradeLimit] = useState('200');
  const [traderScanStatus, setTraderScanStatus] = useState<'active' | 'closed' | 'all'>(
    'active'
  );
  const [traderScanSort, setTraderScanSort] = useState<'volume' | 'trades'>('volume');
  const [traderScanStart, setTraderScanStart] = useState('');
  const [traderScanEnd, setTraderScanEnd] = useState('');
  const [traderScanLoading, setTraderScanLoading] = useState(false);
  const [traderScanError, setTraderScanError] = useState<string | null>(null);
  const [traderScanResults, setTraderScanResults] = useState<TraderScanEntry[]>([]);
  const [traderScanScannedMarkets, setTraderScanScannedMarkets] = useState<number | null>(
    null
  );
  const [marketSearchStatus, setMarketSearchStatus] = useState<
    'active' | 'closed' | 'all'
  >('active');
  const [marketSearchLimit, setMarketSearchLimit] = useState('50');
  const [marketSearchLoading, setMarketSearchLoading] = useState(false);
  const [marketSearchError, setMarketSearchError] = useState<string | null>(null);
  const [marketSearchScanned, setMarketSearchScanned] = useState<number | null>(null);
  const [marketExportLoading, setMarketExportLoading] = useState(false);
  const [marketExportError, setMarketExportError] = useState<string | null>(null);
  const [marketHistorySeriesId, setMarketHistorySeriesId] = useState('');
  const [marketHistorySlug, setMarketHistorySlug] = useState('');
  const [marketHistoryStart, setMarketHistoryStart] = useState('');
  const [marketHistoryEnd, setMarketHistoryEnd] = useState('');
  const [marketHistoryMarketLimit, setMarketHistoryMarketLimit] = useState('200');
  const [marketHistoryTradeLimit, setMarketHistoryTradeLimit] = useState('500');
  const [marketHistoryLoading, setMarketHistoryLoading] = useState(false);
  const [marketHistoryError, setMarketHistoryError] = useState<string | null>(null);
  const [marketHistoryWarning, setMarketHistoryWarning] = useState<string | null>(null);
  const [marketHistorySeriesLoading, setMarketHistorySeriesLoading] =
    useState(false);
  const [marketHistorySeriesError, setMarketHistorySeriesError] = useState<
    string | null
  >(null);
  const [marketHistoryProgress, setMarketHistoryProgress] = useState({
    markets: 0,
    trades: 0,
    totalMarkets: 0,
  });
  const [marketHistoryCancelRequested, setMarketHistoryCancelRequested] =
    useState(false);
  const [liveStreamSlug, setLiveStreamSlug] = useState('');
  const [liveStreamTokenIds, setLiveStreamTokenIds] = useState('');
  const [liveStreamChannels, setLiveStreamChannels] =
    useState<LiveStreamChannelState>(() => ({
      ...LIVE_STREAM_CHANNEL_DEFAULTS,
    }));
  const [liveStreamResolving, setLiveStreamResolving] = useState(false);
  const [liveStreamConnecting, setLiveStreamConnecting] = useState(false);
  const [liveStreamConnected, setLiveStreamConnected] = useState(false);
  const [liveStreamPolling, setLiveStreamPolling] = useState(false);
  const [liveStreamAutoContinue, setLiveStreamAutoContinue] = useState(false);
  const [liveStreamError, setLiveStreamError] = useState<string | null>(null);
  const [liveStreamWarning, setLiveStreamWarning] = useState<string | null>(null);
  const [liveStreamDebug, setLiveStreamDebug] = useState<string[]>([]);
  const [liveStreamMessages, setLiveStreamMessages] = useState<LiveStreamMessage[]>(
    []
  );
  const [liveStreamBookStats, setLiveStreamBookStats] = useState<
    Record<string, LiveStreamBookStats>
  >({});
  const [liveStreamPriceHistory, setLiveStreamPriceHistory] = useState<
    Record<string, LiveStreamPriceHistory>
  >({});
  const [continuousExportSlug, setContinuousExportSlug] = useState('');
  const [continuousExportBatchSize, setContinuousExportBatchSize] = useState('15');
  const [continuousExportTradeLimit, setContinuousExportTradeLimit] = useState('500');
  const [continuousExportRunning, setContinuousExportRunning] = useState(false);
  const [continuousExportStopRequested, setContinuousExportStopRequested] =
    useState(false);
  const [continuousExportError, setContinuousExportError] = useState<string | null>(
    null
  );
  const [continuousExportWarning, setContinuousExportWarning] = useState<
    string | null
  >(null);
  const [continuousExportProgress, setContinuousExportProgress] = useState({
    batch: 0,
    processed: 0,
    currentSlug: '',
    lastSavedSlug: '',
    nextSlug: '',
  });
  const liveStreamStateRef = useRef({
    connected: false,
    polling: false,
    resolving: false,
    connecting: false,
  });

  const isAuthed = Boolean(authToken);
  const isAdmin = authUser?.role === 'admin';

  useEffect(() => {
    const stored = window.localStorage.getItem('dashboard_token');
    if (stored) {
      setAuthToken(stored);
    }
    const storedTheme = window.localStorage.getItem('dashboard_theme');
    if (storedTheme === 'light' || storedTheme === 'dark') {
      setTheme(storedTheme);
    }
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem('dashboard_theme', theme);
  }, [theme]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    const stored = window.localStorage.getItem(LIVE_STREAM_STORAGE_KEY);
    if (!stored) {
      return;
    }
    try {
      const parsed = JSON.parse(stored) as {
        running?: boolean;
        slug?: string;
        tokenIds?: string;
        channels?: string[];
      };
      if (parsed.running) {
        setLiveStreamAutoContinue(true);
        if (parsed.slug) {
          setLiveStreamSlug(parsed.slug);
        }
        if (parsed.tokenIds) {
          setLiveStreamTokenIds(parsed.tokenIds);
        }
        if (Array.isArray(parsed.channels)) {
          setLiveStreamChannels(buildLiveStreamChannelState(parsed.channels));
        }
        handleLiveStreamConnect(null, {
          slug: parsed.slug,
          tokenIds: parsed.tokenIds
            ? parsed.tokenIds.split(/[, ]+/).map((id) => id.trim()).filter(Boolean)
            : undefined,
          channels: Array.isArray(parsed.channels)
            ? parsed.channels
            : undefined,
        });
      }
    } catch {
      // ignore corrupt storage
    }
  }, []);

  useEffect(() => {
    if (liveStreamAutoContinue) {
      persistLiveStreamAutoState(true);
      return;
    }
    persistLiveStreamAutoState(false);
  }, [liveStreamSlug, liveStreamTokenIds, liveStreamChannels, liveStreamAutoContinue]);

  useEffect(() => {
    liveStreamStateRef.current.connected = liveStreamConnected;
  }, [liveStreamConnected]);

  useEffect(() => {
    liveStreamStateRef.current.polling = liveStreamPolling;
  }, [liveStreamPolling]);

  useEffect(() => {
    liveStreamStateRef.current.resolving = liveStreamResolving;
  }, [liveStreamResolving]);

  useEffect(() => {
    liveStreamStateRef.current.connecting = liveStreamConnecting;
  }, [liveStreamConnecting]);

  useEffect(() => {
    if (!authToken) {
      setAuthUser(null);
      return;
    }

    let active = true;
    const loadUser = async () => {
      try {
        const response = await authFetch(authToken, '/api/auth/me');
        if (!response.ok) {
          throw new Error('Not authenticated');
        }
        const data = (await response.json()) as { user?: AuthUser };
        if (active) {
          setAuthUser(data.user ?? null);
          setAuthError(null);
        }
      } catch (_error) {
        if (active) {
          setAuthToken(null);
          setAuthUser(null);
          window.localStorage.removeItem('dashboard_token');
        }
      }
    };

    loadUser();

    return () => {
      active = false;
    };
  }, [authToken]);

  useEffect(() => {
    const wallet = authUser?.walletAddress;
    if (!wallet) {
      return;
    }
    setMonitorInput((current) => (current.trim() ? current : wallet));
  }, [authUser?.walletAddress]);

  useEffect(() => {
    if (authUser?.walletAddress) {
      setWalletEntry(authUser.walletAddress);
    }
  }, [authUser?.walletAddress]);

  useEffect(() => {
    if (!authToken || !isAdmin) {
      return;
    }
    handleAdminRefresh();
  }, [authToken, isAdmin, adminTab]);

  useEffect(() => {
    return () => {
      if (liveStreamSocketRef.current) {
        liveStreamSocketRef.current.close();
        liveStreamSocketRef.current = null;
      }
      stopLiveStreamPolling();
      stopLiveStreamAdvance();
    };
  }, []);

  useEffect(() => {
    if (activePage !== 'market') {
      handleLiveStreamDisconnect();
    }
  }, [activePage]);

  useEffect(() => {
    let active = true;
    const trimmed = monitorInput.trim();

    if (!trimmed) {
      setManualTrades([]);
      setManualError(null);
      setWalletSummary(null);
      setWalletSummaryError(null);
      setWalletSummaryLoading(false);
      return;
    }

    if (!isValidAddress(trimmed)) {
      setManualTrades([]);
      setManualError('Enter a valid wallet address');
      setWalletSummary(null);
      setWalletSummaryError('Enter a valid wallet address');
      setWalletSummaryLoading(false);
      return;
    }

    let intervalId: ReturnType<typeof setInterval> | null = null;
    const fetchActivity = async () => {
      try {
        const response = await fetch(`/api/activity?user=${trimmed}&limit=20`);
        if (!response.ok) {
          throw new Error('Activity fetch failed');
        }
        const data = (await response.json()) as { trades?: TradeActivity[] };
        if (active) {
          setManualTrades(data.trades ?? []);
          setManualError(null);
        }
      } catch (error) {
        if (active) {
          setManualTrades([]);
          setManualError(error instanceof Error ? error.message : 'Activity fetch failed');
        }
      }
    };

    const timeout = setTimeout(() => {
      fetchActivity();
      intervalId = setInterval(fetchActivity, 10000);
    }, 400);

    return () => {
      active = false;
      clearTimeout(timeout);
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [monitorInput]);

  useEffect(() => {
    setExportError(null);
    setExportWarning(null);
  }, [monitorInput]);

  useEffect(() => {
    const trimmed = monitorInput.trim();

    if (!trimmed || !isValidAddress(trimmed)) {
      return;
    }

    let active = true;
    let firstLoad = true;

    const fetchSummary = async () => {
      try {
        if (firstLoad) {
          setWalletSummaryLoading(true);
        }
        const response = await fetch(`/api/wallet-summary?user=${trimmed}`);
        if (!response.ok) {
          throw new Error('Wallet summary fetch failed');
        }
        const data = (await response.json()) as { summary?: WalletSummary };
        if (active) {
          setWalletSummary(data.summary ?? null);
          setWalletSummaryError(null);
        }
      } catch (error) {
        if (active) {
          setWalletSummary(null);
          setWalletSummaryError(error instanceof Error ? error.message : 'Wallet summary fetch failed');
        }
      } finally {
        if (active && firstLoad) {
          setWalletSummaryLoading(false);
          firstLoad = false;
        }
      }
    };

    fetchSummary();
    const interval = setInterval(fetchSummary, 12000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [monitorInput]);

  useEffect(() => {
    const trimmed = monitorInput.trim();

    if (!trimmed || !isValidAddress(trimmed)) {
      setLifetimeSummary(null);
      setLifetimeSummaryError(null);
      setLifetimeSummaryLoading(false);
      return;
    }

    let active = true;
    const controller = new AbortController();

    const timeout = setTimeout(async () => {
      try {
        setLifetimeSummaryLoading(true);
        const response = await fetch(`/api/wallet-lifetime?user=${trimmed}`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error('Lifetime summary fetch failed');
        }
        const data = (await response.json()) as { summary?: LifetimeSummary };
        if (active) {
          setLifetimeSummary(data.summary ?? null);
          setLifetimeSummaryError(null);
        }
      } catch (error) {
        if (active) {
          if (error instanceof DOMException && error.name === 'AbortError') {
            return;
          }
          setLifetimeSummary(null);
          setLifetimeSummaryError(
            error instanceof Error ? error.message : 'Lifetime summary fetch failed'
          );
        }
      } finally {
        if (active) {
          setLifetimeSummaryLoading(false);
        }
      }
    }, 600);

    return () => {
      active = false;
      controller.abort();
      clearTimeout(timeout);
    };
  }, [monitorInput]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    const limitInput = marketSearchLimit.trim();
    const limitValue = limitInput ? parseInt(limitInput, 10) : 50;

    if (limitInput && (!Number.isFinite(limitValue) || limitValue <= 0)) {
      setMarketSearchError('Market limit must be a positive number.');
      setMarketSearchLoading(false);
      setMarketSearchScanned(null);
      setMarkets([]);
      return () => {
        active = false;
        controller.abort();
      };
    }

    const resolvedLimit = Number.isFinite(limitValue) ? Math.min(Math.max(limitValue, 1), 500) : 50;

    const timeout = setTimeout(async () => {
      try {
        setMarketSearchLoading(true);
        setMarketSearchError(null);
        const params = new URLSearchParams();
        if (searchQuery.trim()) {
          params.set('query', searchQuery.trim());
        }
        params.set('status', marketSearchStatus);
        params.set('limit', String(resolvedLimit));
        const response = await fetch(`/api/markets/search?${params.toString()}`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error('Market fetch failed');
        }
        const data = await readJsonResponse<{
          markets?: Market[];
          scanned?: number;
          error?: string;
        }>(response);
        if (active) {
          if (!response.ok) {
            throw new Error(data.error || 'Market fetch failed');
          }
          const list = data.markets ?? [];
          setMarkets(list);
          setMarketSearchScanned(data.scanned ?? null);
          setMarketSearchError(null);
          if (list.length > 0) {
            setSelectedMarketId((current) =>
              list.find((market) => market.id === current) ? current : list[0].id
            );
          }
        }
      } catch (error) {
        if (active) {
          setMarketSearchScanned(null);
          setMarketSearchError(
            error instanceof Error ? error.message : 'Market fetch failed'
          );
          setMarkets([]);
        }
      } finally {
        if (active) {
          setMarketSearchLoading(false);
        }
      }
    }, 350);

    return () => {
      active = false;
      controller.abort();
      clearTimeout(timeout);
    };
  }, [searchQuery, marketSearchLimit, marketSearchStatus]);

  useEffect(() => {
    if (rangeExportError) {
      setRangeExportError(null);
    }
    if (rangeExportWarning) {
      setRangeExportWarning(null);
    }
    if (rangeExportPreview.length > 0) {
      setRangeExportPreview([]);
      setRangeExportPreviewTotal(null);
    }
  }, [rangeExportStart, rangeExportEnd, rangeExportLimit, monitorInput]);

  useEffect(() => {
    if (traderProfileError) {
      setTraderProfileError(null);
    }
    if (traderProfileWarning) {
      setTraderProfileWarning(null);
    }
  }, [traderProfileWallet, traderProfileStart, traderProfileEnd, traderProfileLimit]);

  useEffect(() => {
    if (traderScanError) {
      setTraderScanError(null);
    }
  }, [
    traderScanLimit,
    traderScanMarketLimit,
    traderScanTradeLimit,
    traderScanStatus,
    traderScanSort,
    traderScanStart,
    traderScanEnd,
  ]);

  useEffect(() => {
    if (marketSearchError) {
      setMarketSearchError(null);
    }
    if (marketExportError) {
      setMarketExportError(null);
    }
  }, [searchQuery, marketSearchLimit, marketSearchStatus]);

  useEffect(() => {
    if (resolveError) {
      setResolveError(null);
    }
    if (resolveWarning) {
      setResolveWarning(null);
    }
  }, [resolvePrefix, resolveStart, resolveEnd, resolveLimit]);

  useEffect(() => {
    if (priceHistoryError) {
      setPriceHistoryError(null);
    }
    if (priceHistoryOptions.length > 0) {
      setPriceHistoryOptions([]);
    }
  }, [
    priceHistoryMarket,
    priceHistoryPrefix,
    priceHistoryOutcome,
    priceHistoryOutcomeIndex,
    priceHistoryTokenIds,
    priceHistoryTokenLabels,
    priceHistoryStart,
    priceHistoryEnd,
    priceHistoryFidelity,
    priceHistoryRowLimit,
  ]);

  useEffect(() => {
    let active = true;

    const fetchLeaders = async () => {
      try {
        const response = await fetch('/api/leaderboard?limit=5');
        if (!response.ok) {
          throw new Error('Leaderboard fetch failed');
        }
        const data = (await response.json()) as { leaders?: LeaderEntry[] };
        if (active) {
          setLeaders(data.leaders ?? []);
        }
      } catch (_error) {
        if (active) {
          setLeaders([]);
        }
      }
    };

    fetchLeaders();
    const interval = setInterval(fetchLeaders, 15000);

    return () => {
      active = false;
      clearInterval(interval);
    };
  }, []);

  const selectedMarket = useMemo(() => {
    if (!markets.length) {
      return null;
    }
    return markets.find((market) => market.id === selectedMarketId) ?? markets[0];
  }, [markets, selectedMarketId]);

  useEffect(() => {
    if (parseTokenIdInput(priceHistoryTokenIds).length > 0) {
      return;
    }
    if (priceHistoryMarket.trim()) {
      return;
    }
    if (priceHistoryPrefix.trim()) {
      return;
    }
    if (!selectedMarket) {
      return;
    }
    const fallback = selectedMarket.slug || selectedMarket.clobTokenIds?.[0] || '';
    if (fallback) {
      setPriceHistoryMarket(fallback);
    }
  }, [priceHistoryMarket, priceHistoryPrefix, priceHistoryTokenIds, selectedMarket]);

  const handlePriceHistoryTokenLookup = async () => {
    const slug = priceHistoryMarket.trim();
    if (!slug) {
      setPriceHistoryTokenLookupError('Enter a market slug first.');
      return;
    }
    if (/^\d+$/.test(slug)) {
      setPriceHistoryTokenLookupError('Enter a market slug (not a token id).');
      return;
    }

    setPriceHistoryTokenLookupLoading(true);
    setPriceHistoryTokenLookupError(null);

    try {
      const params = new URLSearchParams();
      params.set('slug', slug);
      const response = await fetch(`/api/markets/token-ids?${params.toString()}`);
      const payload = (await response.json()) as {
        error?: string;
        options?: PriceHistoryOption[];
      };
      if (!response.ok) {
        throw new Error(payload.error || 'Token lookup failed');
      }
      const options = payload.options ?? [];
      if (options.length === 0) {
        throw new Error('No token IDs found for this slug.');
      }

      const tokenIds = Array.from(
        new Set(options.map((option) => option.tokenId).filter(Boolean))
      );
      const outcomes = options.map((option) => option.outcome ?? '').filter(Boolean);
      setPriceHistoryTokenIds(tokenIds.join(', '));
      if (outcomes.length === options.length) {
        setPriceHistoryTokenLabels(outcomes.join(', '));
      } else {
        setPriceHistoryTokenLabels('');
      }
      setPriceHistoryOutcome('');
      setPriceHistoryOutcomeIndex('');
      setPriceHistoryMarket('');
    } catch (error) {
      setPriceHistoryTokenLookupError(
        error instanceof Error ? error.message : 'Token lookup failed'
      );
    } finally {
      setPriceHistoryTokenLookupLoading(false);
    }
  };

  const trimmedWallet = monitorInput.trim();
  const hasValidWallet = isValidAddress(trimmedWallet);
  const monitorRows = mapTrades(manualTrades);
  const positionsWallet = positionsWalletInput.trim() || trimmedWallet;
  const hasPositionsWallet = positionsWallet ? isValidAddress(positionsWallet) : false;
  const positionsLimit = useMemo(() => {
    const parsed = parseInt(positionsPageSize, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 25;
    }
    return Math.min(parsed, 200);
  }, [positionsPageSize]);
  const positionsRows = positionsTab === 'active' ? activePositions : closedPositions;
  const positionsCount =
    positionsTab === 'active' ? activePositionsCount : closedPositionsCount;
  const positionsOffset =
    positionsTab === 'active' ? activePositionsOffset : closedPositionsOffset;
  const activePositionsTotal =
    activePositionsCount !== null && activePositionsCount !== undefined
      ? activePositionsCount
      : activePositions.length;
  const closedPositionsTotal =
    closedPositionsCount !== null && closedPositionsCount !== undefined
      ? closedPositionsCount
      : closedPositions.length;
  const positionsPageStart = positionsRows.length > 0 ? positionsOffset + 1 : 0;
  const positionsPageEnd = positionsOffset + positionsRows.length;
  const positionsHasNext =
    positionsCount !== null && positionsCount !== undefined
      ? positionsPageEnd < positionsCount
      : positionsRows.length === positionsLimit;
  const liveStreamStatusLabel = liveStreamConnected
    ? 'Connected'
    : liveStreamConnecting
      ? 'Connecting'
      : liveStreamResolving
        ? 'Resolving'
        : liveStreamPolling
          ? 'Polling'
          : 'Offline';
  const liveStreamStatusClass = liveStreamConnected
    ? 'status-running'
    : liveStreamConnecting || liveStreamResolving
      ? 'status-starting'
      : liveStreamPolling
        ? 'status-polling'
        : 'status-offline';
  const liveStreamStatsList = Object.values(liveStreamBookStats)
    .sort((a, b) => b.updatedAt - a.updatedAt)
    .slice(0, 6);
  const liveStreamVizTokens = useMemo(() => {
    const tokens = new Set<string>();
    Object.keys(liveStreamPriceHistory).forEach((tokenId) => tokens.add(tokenId));
    Object.keys(liveStreamBookStats).forEach((tokenId) => tokens.add(tokenId));
    return Array.from(tokens).slice(0, 4);
  }, [liveStreamPriceHistory, liveStreamBookStats]);
  const liveStreamLatestPayload = liveStreamMessages[0]?.rawFull ?? '';
  const rangeExportAbortRef = useRef<AbortController | null>(null);
  const rangeExportCancelRef = useRef(false);
  const marketHistoryAbortRef = useRef<AbortController | null>(null);
  const marketHistoryCancelRef = useRef(false);
  const continuousExportAbortRef = useRef<AbortController | null>(null);
  const continuousExportCancelRef = useRef(false);
  const liveStreamSocketRef = useRef<WebSocket | null>(null);
  const liveStreamPollRef = useRef<number | null>(null);
  const liveStreamPollInFlightRef = useRef(false);
  const liveStreamManualDisconnectRef = useRef(false);
  const liveStreamAdvanceRef = useRef<number | null>(null);
  const liveStreamArchiveRef = useRef<LiveStreamMessage[]>([]);
  const liveStreamSnapshotsRef = useRef<LiveStreamPriceSnapshotEntry[]>([]);
  const liveStreamSnapshotTimerRef = useRef<number | null>(null);
  const liveStreamMarketMetaRef = useRef<{
    slug: string;
    prefix: string;
    timestamp: number;
    intervalSeconds: number;
    tokens: string[];
  } | null>(null);
  const liveStreamMarketIdRef = useRef(0);

  useEffect(() => {
    if (!hasValidWallet) {
      setRangeExportAutoFilled(false);
      return;
    }
    if (rangeExportStart.trim()) {
      return;
    }
    if (rangeExportAutoFilled) {
      return;
    }
    setRangeExportAutoFilled(true);
    handleRangeExportUseEarliest();
  }, [hasValidWallet, rangeExportStart, rangeExportAutoFilled]);

  useEffect(() => {
    if (positionsTab === 'active') {
      setActivePositionsOffset(0);
    } else {
      setClosedPositionsOffset(0);
    }
  }, [positionsTab, positionsWallet, positionsLimit]);

  useEffect(() => {
    if (activePage !== 'trader') {
      return;
    }

    if (!positionsWallet || !hasPositionsWallet) {
      setPositionsLoading(false);
      setPositionsError('Enter a valid wallet address.');
      setActivePositions([]);
      setClosedPositions([]);
      setActivePositionsCount(null);
      setClosedPositionsCount(null);
      return;
    }

    let active = true;
    const controller = new AbortController();
    const loadPositions = async () => {
      try {
        setPositionsLoading(true);
        setPositionsError(null);
        const params = new URLSearchParams();
        params.set('user', positionsWallet);
        params.set('limit', String(positionsLimit));
        params.set('offset', String(positionsOffset));
        if (positionsTab === 'active') {
          params.set('sortBy', 'CURRENT');
          params.set('sortDirection', 'DESC');
          params.set('sizeThreshold', '.1');
        } else {
          params.set('sortBy', 'realizedpnl');
          params.set('sortDirection', 'DESC');
        }
        const endpoint = positionsTab === 'active' ? '/api/positions' : '/api/closed-positions';
        const response = await fetch(`${endpoint}?${params.toString()}`, {
          signal: controller.signal,
        });
        const payload = (await response.json()) as {
          positions?: PositionItem[];
          count?: number | null;
          error?: string;
        };
        if (!response.ok) {
          throw new Error(payload.error || 'Positions fetch failed');
        }
        if (!active) {
          return;
        }
        const list = Array.isArray(payload.positions) ? payload.positions : [];
        if (positionsTab === 'active') {
          setActivePositions(list);
          setActivePositionsCount(
            payload.count !== undefined ? payload.count : null
          );
        } else {
          setClosedPositions(list);
          setClosedPositionsCount(
            payload.count !== undefined ? payload.count : null
          );
        }
      } catch (error) {
        if (!active) {
          return;
        }
        if (error instanceof DOMException && error.name === 'AbortError') {
          return;
        }
        setPositionsError(
          error instanceof Error ? error.message : 'Positions fetch failed'
        );
        if (positionsTab === 'active') {
          setActivePositions([]);
          setActivePositionsCount(null);
        } else {
          setClosedPositions([]);
          setClosedPositionsCount(null);
        }
      } finally {
        if (active) {
          setPositionsLoading(false);
        }
      }
    };

    loadPositions();
    return () => {
      active = false;
      controller.abort();
    };
  }, [
    activePage,
    positionsTab,
    positionsWallet,
    positionsLimit,
    positionsOffset,
    hasPositionsWallet,
  ]);

  const activitySummary = useMemo(() => {
    let buyCount = 0;
    let sellCount = 0;
    let buyUsd = 0;
    let sellUsd = 0;
    let lastTimestamp = 0;
    const marketTotals = new Map<string, number>();

    manualTrades.forEach((trade) => {
      const side = normalizeSide(trade.side);
      const usdc = parseNumber(trade.usdcSize);
      if (side === 'buy') {
        buyCount += 1;
        buyUsd += usdc;
      } else {
        sellCount += 1;
        sellUsd += usdc;
      }
      if (trade.timestamp && trade.timestamp > lastTimestamp) {
        lastTimestamp = trade.timestamp;
      }
      const label = trade.title || trade.slug || trade.eventSlug || 'Unknown market';
      marketTotals.set(label, (marketTotals.get(label) || 0) + usdc);
    });

    const topMarkets = Array.from(marketTotals.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([market, usdc]) => ({ market, usdc }));

    return {
      buyCount,
      sellCount,
      buyUsd,
      sellUsd,
      netUsd: sellUsd - buyUsd,
      lastTimestamp,
      topMarkets,
    };
  }, [manualTrades]);

  const hourlyActivity = useMemo(() => {
    const buckets = Array.from({ length: 24 }, () => 0);
    manualTrades.forEach((trade) => {
      if (!trade.timestamp) {
        return;
      }
      const date = new Date(normalizeTimestamp(trade.timestamp));
      const hour = date.getHours();
      buckets[hour] += 1;
    });
    const max = Math.max(...buckets, 1);
    return { buckets, max };
  }, [manualTrades]);

  const winLossDays = useMemo(() => {
    const daily = lifetimeSummary?.daily ?? [];
    let wins = 0;
    let losses = 0;
    let flat = 0;
    daily.forEach((day) => {
      if (day.net > 0) {
        wins += 1;
      } else if (day.net < 0) {
        losses += 1;
      } else {
        flat += 1;
      }
    });
    const total = wins + losses + flat;
    return { wins, losses, flat, total };
  }, [lifetimeSummary]);

  const allTimeStats = useMemo<{
    bestDay: { date: string; net: number } | null;
    worstDay: { date: string; net: number } | null;
    winStreak: number;
    lossStreak: number;
    totalVolume: number | null;
  }>(() => {
    const daily = lifetimeSummary?.daily ?? [];
    let bestDay: { date: string; net: number } | null = null;
    let worstDay: { date: string; net: number } | null = null;
    let winStreak = 0;
    let lossStreak = 0;
    let currentWin = 0;
    let currentLoss = 0;

    daily.forEach((day) => {
      if (day.net > 0) {
        currentWin += 1;
        currentLoss = 0;
        winStreak = Math.max(winStreak, currentWin);
        if (!bestDay || day.net > bestDay.net) {
          bestDay = { date: day.date, net: day.net };
        }
      } else if (day.net < 0) {
        currentLoss += 1;
        currentWin = 0;
        lossStreak = Math.max(lossStreak, currentLoss);
        if (!worstDay || day.net < worstDay.net) {
          worstDay = { date: day.date, net: day.net };
        }
      } else {
        currentWin = 0;
        currentLoss = 0;
      }
    });

    const totalVolume =
      lifetimeSummary ? lifetimeSummary.totalProfit + lifetimeSummary.totalLoss : null;

    return { bestDay, worstDay, winStreak, lossStreak, totalVolume };
  }, [lifetimeSummary]);

  const dailyNetSeries = useMemo(() => {
    const daily = lifetimeSummary?.daily ?? [];
    if (daily.length === 0) {
      return { recent: [], min: 0, max: 0 };
    }
    const recent = daily.slice(-14);
    const values = recent.map((day) => day.net);
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (min === max) {
      min -= 1;
      max += 1;
    }
    return { recent, min, max };
  }, [lifetimeSummary]);

  const dailyNetSparkline = useMemo(() => {
    const width = 240;
    const height = 80;
    const padding = 6;
    if (dailyNetSeries.recent.length < 2) {
      return { width, height, points: '', zeroY: null };
    }
    const range = dailyNetSeries.max - dailyNetSeries.min || 1;
    const step =
      dailyNetSeries.recent.length > 1
        ? (width - padding * 2) / (dailyNetSeries.recent.length - 1)
        : 0;
    const points = dailyNetSeries.recent
      .map((day, index) => {
        const x = padding + index * step;
        const normalized = (day.net - dailyNetSeries.min) / range;
        const y = height - padding - normalized * (height - padding * 2);
        return `${x},${y}`;
      })
      .join(' ');
    const zeroY =
      dailyNetSeries.min < 0 && dailyNetSeries.max > 0
        ? height -
          padding -
          ((0 - dailyNetSeries.min) / range) * (height - padding * 2)
        : null;
    return { width, height, points, zeroY };
  }, [dailyNetSeries]);

  const hasDataIssue = Boolean(
    hasValidWallet && (manualError || walletSummaryError || lifetimeSummaryError)
  );
  const statusState = hasValidWallet ? (hasDataIssue ? 'offline' : 'running') : 'loading';
  const statusLabel = hasValidWallet ? (hasDataIssue ? 'Data issue' : 'Connected') : 'Awaiting wallet';

  const handleAuthSubmit = async (event?: FormEvent) => {
    event?.preventDefault();
    if (isAuthed || authLoading) {
      return;
    }
    const email = authEmail.trim();
    const password = authPassword.trim();
    if (!email || !password) {
      setAuthError('Enter your email and password.');
      return;
    }

    setAuthLoading(true);
    try {
      const recaptchaToken = await executeRecaptcha(
        authMode === 'signup' ? 'signup' : 'login'
      );
      const response = await fetch(
        authMode === 'signup' ? '/api/auth/register' : '/api/auth/login',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password, recaptchaToken }),
        }
      );
      if (!response.ok) {
        const payload = (await response.json()) as { error?: string };
        throw new Error(payload.error || 'Authentication failed');
      }
      const data = (await response.json()) as { token: string; user?: AuthUser };
      setAuthToken(data.token);
      window.localStorage.setItem('dashboard_token', data.token);
      setAuthUser(data.user ?? null);
      setAuthError(null);
      setAuthEmail('');
      setAuthPassword('');
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : 'Authentication failed');
    } finally {
      setAuthLoading(false);
    }
  };

  const handleLogout = async () => {
    if (!authToken) {
      return;
    }
    await authFetch(authToken, '/api/auth/logout', { method: 'POST' });
    setAuthToken(null);
    setAuthUser(null);
    setAdminUsers([]);
    setAdminActivity([]);
    setAdminError(null);
    setAdminTab('users');
    window.localStorage.removeItem('dashboard_token');
  };

  const handleWalletSave = async (overrideWallet?: string) => {
    if (!authToken) {
      return;
    }
    const wallet = (overrideWallet ?? walletEntry).trim();
    if (!wallet) {
      setWalletSaveError('Enter a wallet address to save.');
      return;
    }
    if (!isValidAddress(wallet)) {
      setWalletSaveError('Enter a valid wallet address.');
      return;
    }

    setWalletSaveLoading(true);
    try {
      const response = await authFetch(authToken, '/api/auth/wallet', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ walletAddress: wallet }),
      });
      if (!response.ok) {
        const payload = (await response.json()) as { error?: string };
        throw new Error(payload.error || 'Failed to save wallet');
      }
      const data = (await response.json()) as { user?: AuthUser };
      setAuthUser(data.user ?? null);
      setMonitorInput(wallet);
      setWalletSaveError(null);
    } catch (error) {
      setWalletSaveError(error instanceof Error ? error.message : 'Failed to save wallet');
    } finally {
      setWalletSaveLoading(false);
    }
  };

  const handleAdminRefresh = async () => {
    if (!authToken || !isAdmin) {
      return;
    }
    setAdminLoading(true);
    setAdminError(null);
    try {
      if (adminTab === 'users') {
        const response = await authFetch(authToken, '/api/admin/users?limit=100');
        if (!response.ok) {
          throw new Error('Failed to load users');
        }
        const data = (await response.json()) as { users?: AuthUser[] };
        setAdminUsers(data.users ?? []);
      } else {
        const response = await authFetch(authToken, '/api/admin/activity?limit=100');
        if (!response.ok) {
          throw new Error('Failed to load activity');
        }
        const data = (await response.json()) as { logs?: AdminActivity[] };
        setAdminActivity(data.logs ?? []);
      }
    } catch (error) {
      setAdminError(error instanceof Error ? error.message : 'Admin data load failed');
    } finally {
      setAdminLoading(false);
    }
  };

  const handleRoleToggle = async (user: AuthUser) => {
    if (!authToken || !isAdmin) {
      return;
    }
    const nextRole = user.role === 'admin' ? 'user' : 'admin';
    setAdminLoading(true);
    setAdminError(null);
    try {
      const response = await authFetch(authToken, `/api/admin/users/${user.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: nextRole }),
      });
      if (!response.ok) {
        const payload = (await response.json()) as { error?: string };
        throw new Error(payload.error || 'Failed to update role');
      }
      const data = (await response.json()) as { user?: AuthUser };
      if (data.user) {
        setAdminUsers((prev) =>
          prev.map((entry) => (entry.id === data.user?.id ? data.user : entry))
        );
      }
    } catch (error) {
      setAdminError(error instanceof Error ? error.message : 'Role update failed');
    } finally {
      setAdminLoading(false);
    }
  };

  const getTradeKey = (trade: TradeActivity) =>
    trade.id || trade.transactionHash || `${trade.userAddress}-${trade.timestamp ?? ''}-${trade.usdcSize ?? ''}`;

  const handlePositionsPrev = () => {
    if (positionsTab === 'active') {
      setActivePositionsOffset((prev) => Math.max(0, prev - positionsLimit));
    } else {
      setClosedPositionsOffset((prev) => Math.max(0, prev - positionsLimit));
    }
  };

  const handlePositionsNext = () => {
    const nextOffset = positionsOffset + positionsLimit;
    const total = positionsCount;
    if (total !== null && total !== undefined && nextOffset >= total) {
      return;
    }
    if (positionsTab === 'active') {
      setActivePositionsOffset(nextOffset);
    } else {
      setClosedPositionsOffset(nextOffset);
    }
  };

  const handleRangeExportCancel = () => {
    if (!rangeExportLoading) {
      return;
    }
    rangeExportCancelRef.current = true;
    setRangeExportCancelRequested(true);
    setRangeExportWarning((prev) =>
      prev ? `${prev} Cancel requested; exporting fetched data.` : 'Cancel requested; exporting fetched data.'
    );
    if (rangeExportAbortRef.current) {
      rangeExportAbortRef.current.abort();
    }
  };

  const handleMarketHistoryCancel = () => {
    if (!marketHistoryLoading) {
      return;
    }
    marketHistoryCancelRef.current = true;
    setMarketHistoryCancelRequested(true);
    setMarketHistoryWarning((prev) =>
      prev ? `${prev} Cancel requested; exporting fetched data.` : 'Cancel requested; exporting fetched data.'
    );
    if (marketHistoryAbortRef.current) {
      marketHistoryAbortRef.current.abort();
    }
  };

  const handleMarketSeriesLookup = async () => {
    const slug = marketHistorySlug.trim();
    if (!slug) {
      setMarketHistorySeriesError('Enter a market slug.');
      return;
    }

    setMarketHistorySeriesLoading(true);
    setMarketHistorySeriesError(null);

    try {
      const response = await fetch(
        `/api/markets/series-id?slug=${encodeURIComponent(slug)}`
      );
      const payload = await readJsonResponse<{ seriesId?: string; error?: string }>(
        response
      );
      if (!response.ok) {
        throw new Error(payload.error || 'Series lookup failed');
      }
      if (!payload.seriesId) {
        throw new Error('series_id not found for slug');
      }
      setMarketHistorySeriesId(payload.seriesId);
    } catch (error) {
      setMarketHistorySeriesError(
        error instanceof Error ? error.message : 'Series lookup failed'
      );
    } finally {
      setMarketHistorySeriesLoading(false);
    }
  };

  const appendContinuousExportWarning = (message: string) => {
    setContinuousExportWarning((prev) => (prev ? `${prev} ${message}` : message));
  };

  const fetchContinuousJson = async <T,>(url: string) => {
    const controller = new AbortController();
    continuousExportAbortRef.current = controller;
    try {
      const response = await fetch(url, { signal: controller.signal });
      const payload = await readJsonResponse<T>(response);
      return { response, payload };
    } finally {
      if (continuousExportAbortRef.current === controller) {
        continuousExportAbortRef.current = null;
      }
    }
  };

  const fetchMarketForSlug = async (slug: string) => {
    const params = new URLSearchParams();
    params.set('query', slug);
    params.set('status', 'all');
    params.set('limit', '10');
    const { response, payload } = await fetchContinuousJson<{
      markets?: Market[];
      error?: string;
    }>(`/api/markets/search?${params.toString()}`);
    if (!response.ok) {
      throw new Error(payload.error || 'Market lookup failed');
    }
    const list = payload.markets ?? [];
    const exact = list.find(
      (entry) => entry.slug?.toLowerCase() === slug.toLowerCase()
    );
    return exact ?? list[0] ?? null;
  };

  const fetchTokenOptionsForSlug = async (slug: string) => {
    const { response, payload } = await fetchContinuousJson<{
      slug?: string;
      question?: string;
      options?: PriceHistoryOption[];
      error?: string;
    }>(`/api/markets/token-ids?slug=${encodeURIComponent(slug)}`);
    if (!response.ok) {
      throw new Error(payload.error || 'Token lookup failed');
    }
    return {
      options: payload.options ?? [],
      question: payload.question,
    };
  };

  const fetchSeriesIdForSlug = async (slug: string) => {
    const { response, payload } = await fetchContinuousJson<{
      seriesId?: string;
      error?: string;
    }>(`/api/markets/series-id?slug=${encodeURIComponent(slug)}`);
    if (!response.ok) {
      throw new Error(payload.error || 'Series lookup failed');
    }
    if (!payload.seriesId) {
      throw new Error('series_id not found for slug');
    }
    return payload.seriesId;
  };

  const exportMarketSnapshot = async (slug: string, tradeLimit: number) => {
    let market: Market | null = null;
    let seriesId: string | null = null;
    let tokenOptions: PriceHistoryOption[] = [];
    let priceSeries: PriceHistorySeries[] = [];
    let trades: MarketTradeEntry[] = [];

    try {
      market = await fetchMarketForSlug(slug);
      if (market?.slug && market.slug !== slug) {
        appendContinuousExportWarning(
          `Exact slug not found for ${slug}; using ${market.slug}.`
        );
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') {
        throw error;
      }
      appendContinuousExportWarning(
        `Market lookup failed for ${slug}. Exported partial data.`
      );
    }

    try {
      const tokenResult = await fetchTokenOptionsForSlug(slug);
      tokenOptions = tokenResult.options;
      if (!market && tokenResult.question) {
        const outcomes = tokenOptions
          .map((option) => option.outcome ?? '')
          .filter((label) => label.length > 0);
        const clobTokenIds = tokenOptions.map((option) => option.tokenId);
        market = {
          id: 0,
          question: tokenResult.question,
          slug,
          liquidity: 0,
          outcomes,
          outcomePrices: [],
          clobTokenIds,
        };
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') {
        throw error;
      }
      appendContinuousExportWarning(
        `Token lookup failed for ${slug}. Exported without token list.`
      );
    }

    try {
      seriesId = await fetchSeriesIdForSlug(slug);
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') {
        throw error;
      }
      appendContinuousExportWarning(
        `Series lookup failed for ${slug}. Exported without series id.`
      );
    }

    const fallbackTokens =
      tokenOptions.length > 0
        ? tokenOptions
        : market?.clobTokenIds?.map((tokenId, index) => ({
            tokenId,
            outcome: market?.outcomes?.[index],
            outcomeIndex: index,
          })) ?? [];

    if (fallbackTokens.length > 0) {
      for (const token of fallbackTokens) {
        if (continuousExportCancelRef.current) {
          break;
        }
        try {
          const params = new URLSearchParams();
          params.set('tokenId', token.tokenId);
          const { response, payload } = await fetchContinuousJson<{
            error?: string;
            history?: unknown;
            priceToBeat?: number | null;
            currentPrice?: number | null;
            liquidityClob?: number | null;
          }>(`/api/price-history?${params.toString()}`);
          if (!response.ok) {
            throw new Error(payload.error || 'Price history fetch failed');
          }
          const points = extractPriceHistoryPoints(payload.history);
          priceSeries.push({
            tokenId: token.tokenId,
            slug,
            outcome: token.outcome,
            outcomeIndex: token.outcomeIndex,
            priceToBeat: payload.priceToBeat ?? null,
            currentPrice: payload.currentPrice ?? null,
            liquidityClob: payload.liquidityClob ?? null,
            points,
          });
        } catch (error) {
          if (error instanceof DOMException && error.name === 'AbortError') {
            throw error;
          }
          appendContinuousExportWarning(
            `Price history failed for token ${token.tokenId}.`
          );
        }
      }
    } else {
      try {
        const { response, payload } = await fetchContinuousJson<{
          error?: string;
          tokenId?: string;
          outcome?: string;
          outcomeIndex?: number;
          history?: unknown;
          priceToBeat?: number | null;
          currentPrice?: number | null;
          liquidityClob?: number | null;
        }>(`/api/price-history?slug=${encodeURIComponent(slug)}`);
        if (!response.ok) {
          throw new Error(payload.error || 'Price history fetch failed');
        }
        if (payload.tokenId) {
          const points = extractPriceHistoryPoints(payload.history);
          const token = {
            tokenId: payload.tokenId,
            outcome: payload.outcome,
            outcomeIndex: payload.outcomeIndex,
          };
          tokenOptions = [token];
          priceSeries = [
            {
              tokenId: payload.tokenId,
              slug,
              outcome: payload.outcome,
              outcomeIndex: payload.outcomeIndex,
              priceToBeat: payload.priceToBeat ?? null,
              currentPrice: payload.currentPrice ?? null,
              liquidityClob: payload.liquidityClob ?? null,
              points,
            },
          ];
        }
      } catch (error) {
        if (error instanceof DOMException && error.name === 'AbortError') {
          throw error;
        }
        appendContinuousExportWarning(
          `Price history failed for ${slug}. Exported without history.`
        );
      }
    }

    if (market?.conditionId) {
      try {
        const tradeParams = new URLSearchParams();
        tradeParams.set('market', market.conditionId);
        tradeParams.set('limit', String(Math.min(tradeLimit, 500)));
        const { response, payload } = await fetchContinuousJson<{
          trades?: MarketTradeEntry[];
          error?: string;
        }>(`/api/markets/trades?${tradeParams.toString()}`);
        if (!response.ok) {
          throw new Error(payload.error || 'Trade fetch failed');
        }
        trades = payload.trades ?? [];
      } catch (error) {
        if (error instanceof DOMException && error.name === 'AbortError') {
          throw error;
        }
        appendContinuousExportWarning(
          `Trades failed for ${slug}. Exported without trades.`
        );
      }
    } else {
      appendContinuousExportWarning(`No condition id for ${slug}. Trades skipped.`);
    }

    const workbook = buildMarketSnapshotWorkbook(
      slug,
      market,
      seriesId,
      tokenOptions,
      priceSeries,
      trades
    );
    const fileName = `market-${slug}-${new Date().toISOString().slice(0, 10)}.xlsx`;
    downloadWorkbook(workbook, fileName);
  };

  const handleContinuousExportStop = () => {
    if (!continuousExportRunning) {
      return;
    }
    continuousExportCancelRef.current = true;
    setContinuousExportStopRequested(true);
    if (continuousExportAbortRef.current) {
      continuousExportAbortRef.current.abort();
    }
  };

  const handleContinuousExportStart = async () => {
    if (continuousExportRunning) {
      return;
    }
    const slugInput = continuousExportSlug.trim();
    const parsed = parseMarketSlugSequence(slugInput);
    if (!parsed) {
      setContinuousExportError(
        'Slug must end with a timestamp and include an interval like -15m.'
      );
      return;
    }

    const batchParsed = parseInt(continuousExportBatchSize.trim(), 10);
    if (!Number.isFinite(batchParsed) || batchParsed <= 0) {
      setContinuousExportError('Markets per batch must be a positive number.');
      return;
    }

    const tradeLimitParsed = parseInt(continuousExportTradeLimit.trim(), 10);
    if (!Number.isFinite(tradeLimitParsed) || tradeLimitParsed <= 0) {
      setContinuousExportError('Trades per market must be a positive number.');
      return;
    }

    setContinuousExportRunning(true);
    setContinuousExportStopRequested(false);
    setContinuousExportError(null);
    setContinuousExportWarning(null);
    setContinuousExportProgress({
      batch: 0,
      processed: 0,
      currentSlug: '',
      lastSavedSlug: '',
      nextSlug: '',
    });
    continuousExportCancelRef.current = false;

    let currentTimestamp = parsed.timestamp;
    let batch = 0;
    let processed = 0;
    let lastSavedSlug = '';

    try {
      while (!continuousExportCancelRef.current) {
        batch += 1;
        for (let i = 0; i < batchParsed; i += 1) {
          if (continuousExportCancelRef.current) {
            break;
          }
          const slug = `${parsed.prefix}-${currentTimestamp}`;
          const nextSlug = `${parsed.prefix}-${currentTimestamp + parsed.intervalSeconds}`;
          setContinuousExportProgress({
            batch,
            processed,
            currentSlug: slug,
            lastSavedSlug,
            nextSlug,
          });

          try {
            await exportMarketSnapshot(slug, tradeLimitParsed);
            processed += 1;
            lastSavedSlug = slug;
            setContinuousExportProgress({
              batch,
              processed,
              currentSlug: slug,
              lastSavedSlug,
              nextSlug,
            });
          } catch (error) {
            if (error instanceof DOMException && error.name === 'AbortError') {
              continuousExportCancelRef.current = true;
              break;
            }
            appendContinuousExportWarning(
              error instanceof Error ? error.message : `Export failed for ${slug}.`
            );
          }

          currentTimestamp += parsed.intervalSeconds;
          setContinuousExportSlug(`${parsed.prefix}-${currentTimestamp}`);
          if (!continuousExportCancelRef.current) {
            await wait(250);
          }
        }
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') {
        setContinuousExportWarning((prev) =>
          prev ? `${prev} Export stopped early.` : 'Export stopped early.'
        );
      } else {
        setContinuousExportError(
          error instanceof Error ? error.message : 'Continuous export failed.'
        );
      }
    } finally {
      continuousExportAbortRef.current = null;
      continuousExportCancelRef.current = false;
      setContinuousExportRunning(false);
      setContinuousExportStopRequested(false);
    }
  };

  const handleLiveStreamClear = () => {
    setLiveStreamMessages([]);
    setLiveStreamBookStats({});
    setLiveStreamPriceHistory({});
  };

  const fetchLiveStreamPriceSnapshot = async (slug: string) => {
    if (!slug) {
      return { priceToBeat: null, currentPrice: null };
    }
    const response = await fetch(
      `/api/markets/price-snapshot?slug=${encodeURIComponent(slug)}`
    );
    const payload = (await response.json()) as {
      error?: string;
      priceToBeat?: number | null;
      currentPrice?: number | null;
    };
    if (!response.ok) {
      throw new Error(payload.error || 'Price snapshot fetch failed.');
    }
    return {
      priceToBeat: payload.priceToBeat ?? null,
      currentPrice: payload.currentPrice ?? null,
    };
  };

  const clearLiveStreamSnapshots = () => {
    liveStreamSnapshotsRef.current = [];
    if (liveStreamSnapshotTimerRef.current !== null) {
      window.clearInterval(liveStreamSnapshotTimerRef.current);
      liveStreamSnapshotTimerRef.current = null;
    }
  };

  const startLiveStreamSnapshots = (slug: string, marketId: number) => {
    clearLiveStreamSnapshots();
    if (!slug) {
      return;
    }
    const capture = async () => {
      if (marketId !== liveStreamMarketIdRef.current) {
        return;
      }
      try {
        const snapshot = await fetchLiveStreamPriceSnapshot(slug);
        liveStreamSnapshotsRef.current.push({
          timestampMs: Date.now(),
          priceToBeat: snapshot.priceToBeat ?? null,
          currentPrice: snapshot.currentPrice ?? null,
        });
      } catch (_error) {
        // Skip snapshot errors; export will still include latest.
      }
    };
    capture();
    liveStreamSnapshotTimerRef.current = window.setInterval(
      capture,
      LIVE_STREAM_SNAPSHOT_INTERVAL_MS
    );
  };

  const appendLiveStreamMessage = (
    message: LiveStreamMessage,
    bookStats?: LiveStreamBookStats | null,
    marketId?: number
  ) => {
    if (
      marketId !== undefined &&
      marketId !== null &&
      marketId !== liveStreamMarketIdRef.current
    ) {
      return;
    }
    setLiveStreamMessages((prev) => {
      const next = [message, ...prev];
      return next.slice(0, 200);
    });
    liveStreamArchiveRef.current.push(message);
    if (liveStreamArchiveRef.current.length > LIVE_STREAM_ARCHIVE_LIMIT) {
      liveStreamArchiveRef.current.splice(
        0,
        liveStreamArchiveRef.current.length - LIVE_STREAM_ARCHIVE_LIMIT
      );
    }
    if (bookStats) {
      setLiveStreamBookStats((prev) => ({
        ...prev,
        [bookStats.tokenId]: {
          ...bookStats,
          updatedAt: Date.now(),
        },
      }));
    }
    if (message.tokenId) {
      const tokenId = message.tokenId;
      const priceValue = message.price;
      const bidValue =
        bookStats?.bestBid !== undefined && bookStats?.bestBid !== null
          ? bookStats.bestBid
          : null;
      const askValue =
        bookStats?.bestAsk !== undefined && bookStats?.bestAsk !== null
          ? bookStats.bestAsk
          : null;

      if (
        priceValue !== null ||
        bidValue !== null ||
        askValue !== null
      ) {
        setLiveStreamPriceHistory((prev) => {
          const existing = prev[tokenId];
          const points = existing?.points ?? [];
          const bidPoints = existing?.bidPoints ?? [];
          const askPoints = existing?.askPoints ?? [];
          const nextPoints =
            priceValue !== null && priceValue !== undefined
              ? [...points, priceValue].slice(-LIVE_STREAM_PRICE_POINTS)
              : points;
          const nextBidPoints =
            bidValue !== null
              ? [...bidPoints, bidValue].slice(-LIVE_STREAM_PRICE_POINTS)
              : bidPoints;
          const nextAskPoints =
            askValue !== null
              ? [...askPoints, askValue].slice(-LIVE_STREAM_PRICE_POINTS)
              : askPoints;

          return {
            ...prev,
            [tokenId]: {
              points: nextPoints,
              bidPoints: nextBidPoints,
              askPoints: nextAskPoints,
              updatedAt: Date.now(),
            },
          };
        });
      }
    }
  };

  const appendLiveStreamRaw = (raw: string, marketId?: number) => {
    const { message, bookStats } = buildLiveStreamMessage(raw);
    appendLiveStreamMessage(message, bookStats, marketId);
  };

  const stopLiveStreamPolling = () => {
    if (liveStreamPollRef.current !== null) {
      window.clearInterval(liveStreamPollRef.current);
      liveStreamPollRef.current = null;
    }
    liveStreamPollInFlightRef.current = false;
    liveStreamStateRef.current.polling = false;
    setLiveStreamPolling(false);
  };

  const persistLiveStreamAutoState = (
    running: boolean,
    slug?: string,
    tokenIds?: string,
    channels?: string[]
  ) => {
    if (typeof window === 'undefined') {
      return;
    }
    if (!running) {
      window.localStorage.removeItem(LIVE_STREAM_STORAGE_KEY);
      return;
    }
    const payload = {
      running,
      slug: slug ?? liveStreamSlug,
      tokenIds: tokenIds ?? liveStreamTokenIds,
      channels: channels ?? getLiveStreamChannelList(liveStreamChannels),
    };
    window.localStorage.setItem(LIVE_STREAM_STORAGE_KEY, JSON.stringify(payload));
  };

  const fetchClobBook = async (tokenId: string) => {
    const response = await fetch(
      `/api/clob/book?tokenId=${encodeURIComponent(tokenId)}`
    );
    const payload = (await response.json()) as {
      error?: string;
      tokenId?: string;
      bids?: Array<[number, number]>;
      asks?: Array<[number, number]>;
      bestBid?: number | null;
      bestAsk?: number | null;
      mid?: number | null;
      spread?: number | null;
    };
    if (!response.ok) {
      throw new Error(payload.error || 'Order book fetch failed.');
    }
    return payload;
  };

  const fetchClobLastTrade = async (tokenId: string) => {
    const response = await fetch(
      `/api/clob/last-trade?tokenId=${encodeURIComponent(tokenId)}`
    );
    const payload = (await response.json()) as {
      error?: string;
      tokenId?: string;
      price?: number | null;
      side?: string | null;
    };
    if (!response.ok) {
      throw new Error(payload.error || 'Last trade fetch failed.');
    }
    return payload;
  };

  const pollLiveStreamOnce = async (
    tokens: string[],
    channels: string[],
    marketId?: number
  ) => {
    if (liveStreamPollInFlightRef.current) {
      return;
    }
    liveStreamPollInFlightRef.current = true;
    try {
      const tasks = tokens.map(async (tokenId) => {
        if (channels.includes('book')) {
          try {
            const book = await fetchClobBook(tokenId);
            appendLiveStreamRaw(
              JSON.stringify({
                channel: 'book',
                token_id: tokenId,
                bids: book.bids ?? [],
                asks: book.asks ?? [],
                bestBid: book.bestBid ?? null,
                bestAsk: book.bestAsk ?? null,
                mid: book.mid ?? null,
                spread: book.spread ?? null,
              }),
              marketId
            );
          } catch (error) {
            const message =
              error instanceof Error ? error.message : 'Order book fetch failed.';
            if (!/not found/i.test(message)) {
              setLiveStreamWarning((prev) => prev ?? message);
            }
          }
        }
        if (channels.includes('market')) {
          try {
            const trade = await fetchClobLastTrade(tokenId);
            if (trade.price !== null && trade.price !== undefined) {
              appendLiveStreamRaw(
                JSON.stringify({
                  channel: 'market',
                  token_id: tokenId,
                  price: trade.price,
                  side: trade.side ?? undefined,
                }),
                marketId
              );
            }
          } catch (error) {
            const message =
              error instanceof Error ? error.message : 'Last trade fetch failed.';
            setLiveStreamWarning((prev) => prev ?? message);
          }
        }
      });
      await Promise.allSettled(tasks);
    } finally {
      liveStreamPollInFlightRef.current = false;
    }
  };

  const startLiveStreamPolling = (
    tokens: string[],
    channels: string[],
    reason: string,
    marketId?: number
  ) => {
    if (liveStreamPollRef.current !== null) {
      return;
    }
    setLiveStreamError(null);
    setLiveStreamWarning(
      reason
        ? `${reason} Using REST polling every ${LIVE_STREAM_POLL_INTERVAL_MS / 1000}s.`
        : `Using REST polling every ${LIVE_STREAM_POLL_INTERVAL_MS / 1000}s.`
    );
    liveStreamStateRef.current.polling = true;
    setLiveStreamPolling(true);
    pollLiveStreamOnce(tokens, channels, marketId);
    liveStreamPollRef.current = window.setInterval(() => {
      pollLiveStreamOnce(tokens, channels, marketId);
    }, LIVE_STREAM_POLL_INTERVAL_MS);
  };

  const clearLiveStreamAdvanceTimer = () => {
    if (liveStreamAdvanceRef.current !== null) {
      window.clearTimeout(liveStreamAdvanceRef.current);
      liveStreamAdvanceRef.current = null;
    }
  };

  const stopLiveStreamAdvance = () => {
    clearLiveStreamAdvanceTimer();
    liveStreamMarketMetaRef.current = null;
    clearLiveStreamSnapshots();
  };

  const scheduleLiveStreamAdvance = (meta: {
    slug: string;
    prefix: string;
    timestamp: number;
    intervalSeconds: number;
    tokens: string[];
  }) => {
    clearLiveStreamAdvanceTimer();
    const endMs = (meta.timestamp + meta.intervalSeconds) * 1000;
    const delay = Math.max(endMs - Date.now(), 1000);
    liveStreamAdvanceRef.current = window.setTimeout(() => {
      handleLiveStreamAutoAdvance();
    }, delay + LIVE_STREAM_ADVANCE_BUFFER_MS);
  };

  const exportLiveStreamArchive = async (
    meta: {
      slug: string;
      timestamp: number;
      intervalSeconds: number;
      tokens: string[];
    } | null
  ) => {
    if (!meta) {
      return;
    }
    let priceSnapshot: { priceToBeat: number | null; currentPrice: number | null } | undefined;
    try {
      priceSnapshot = await fetchLiveStreamPriceSnapshot(meta.slug);
    } catch {
      priceSnapshot = undefined;
    }
    if (priceSnapshot) {
      liveStreamSnapshotsRef.current.push({
        timestampMs: Date.now(),
        priceToBeat: priceSnapshot.priceToBeat ?? null,
        currentPrice: priceSnapshot.currentPrice ?? null,
      });
    }
    const messages = liveStreamArchiveRef.current.slice();
    const snapshots = liveStreamSnapshotsRef.current.slice();
    const workbook = buildLiveStreamWorkbook(
      meta.slug || 'market',
      meta.tokens,
      messages,
      {
        timestamp: meta.timestamp,
        intervalSeconds: meta.intervalSeconds,
      },
      priceSnapshot,
      snapshots
    );
    const fileName = `live-stream-${meta.slug || 'market'}-${new Date()
      .toISOString()
      .slice(0, 10)}.xlsx`;
    downloadWorkbook(workbook, fileName);
  };

  const registerLiveStreamMarket = (slug: string, tokens: string[]) => {
    liveStreamMarketIdRef.current += 1;
    const marketId = liveStreamMarketIdRef.current;
    liveStreamArchiveRef.current = [];
    liveStreamSnapshotsRef.current = [];
    setLiveStreamPriceHistory({});
    const parsed = parseMarketSlugSequence(slug);
    if (parsed) {
      const meta = {
        slug,
        prefix: parsed.prefix,
        timestamp: parsed.timestamp,
        intervalSeconds: parsed.intervalSeconds,
        tokens,
      };
      liveStreamMarketMetaRef.current = meta;
      scheduleLiveStreamAdvance(meta);
    } else {
      liveStreamMarketMetaRef.current = null;
      clearLiveStreamAdvanceTimer();
    }
    startLiveStreamSnapshots(slug, marketId);
    return marketId;
  };

  const handleLiveStreamAutoAdvance = async () => {
    clearLiveStreamAdvanceTimer();
    const meta = liveStreamMarketMetaRef.current;
    if (!meta) {
      return;
    }
    const currentState = liveStreamStateRef.current;
    if (currentState.resolving || currentState.connecting) {
      scheduleLiveStreamAdvance(meta);
      return;
    }
    if (!currentState.connected && !currentState.polling) {
      stopLiveStreamAdvance();
      return;
    }

    await exportLiveStreamArchive(meta);

    const nextTimestamp = meta.timestamp + meta.intervalSeconds;
    const nextSlug = `${meta.prefix}-${nextTimestamp}`;
    liveStreamArchiveRef.current = [];
    setLiveStreamMessages([]);
    setLiveStreamBookStats({});
    setLiveStreamPriceHistory({});
    setLiveStreamSlug(nextSlug);
    setLiveStreamTokenIds('');

    handleLiveStreamDisconnect();
    await wait(200);
    await handleLiveStreamConnect(null, { slug: nextSlug });
  };

  const tryFetchTokenOptionsForSlug = async (slug: string) => {
    const { response, payload } = await fetchContinuousJson<{
      slug?: string;
      question?: string;
      options?: PriceHistoryOption[];
      error?: string;
    }>(`/api/markets/token-ids?slug=${encodeURIComponent(slug)}`);
    const options = payload.options ?? [];
    return {
      ok: response.ok,
      status: response.status,
      error: payload.error,
      options,
      question: payload.question,
    };
  };

  const resolveLiveStreamTokens = async (
    slugOverride?: string,
    tokenIdsOverride?: string[]
  ) => {
    const debug: string[] = [];
    const tokenIds = tokenIdsOverride ?? parseTokenIdInput(liveStreamTokenIds);
    if (tokenIds.length > 0) {
      setLiveStreamDebug([]);
      return {
        tokens: tokenIds,
        slug: slugOverride?.trim() || liveStreamSlug.trim(),
      };
    }
    const slugInput = (slugOverride ?? liveStreamSlug).trim();
    if (!slugInput) {
      setLiveStreamDebug([]);
      throw new Error('Enter a slug or token IDs.');
    }
    debug.push(`Slug input: ${slugInput}`);
    const normalized = normalizeSlugTimestamp(slugInput);
    const slug = normalized.slug;
    let resolvedSlug = slug;
    if (normalized.changed) {
      setLiveStreamSlug(slug);
      setLiveStreamWarning(`Normalized slug timestamp to seconds: ${slug}.`);
      debug.push(`Normalized slug: ${slug}`);
    }
    try {
      const tokenResult = await tryFetchTokenOptionsForSlug(slug);
      debug.push(
        `Token lookup (${slug}): ${tokenResult.status} ${tokenResult.ok ? 'ok' : 'fail'}`
      );
      if (tokenResult.error) {
        debug.push(`Token lookup error: ${tokenResult.error}`);
      }
      if (tokenResult.ok && tokenResult.options.length > 0) {
        const resolved = tokenResult.options.map((option) => option.tokenId);
        setLiveStreamTokenIds(resolved.join(', '));
        setLiveStreamDebug(debug);
        return { tokens: resolved, slug: resolvedSlug };
      }
    } catch (_error) {
      debug.push(`Token lookup exception for ${slug}.`);
      // Fall through to prefix resolve.
    }

    const prefix = stripSlugTimestamp(slug);
    if (!prefix) {
      setLiveStreamDebug(debug);
      throw new Error('No token IDs found for slug.');
    }
    debug.push(`Prefix: ${prefix}`);
    const intervalSeconds = parsePrefixIntervalSeconds(prefix);
    if (intervalSeconds) {
      const nowTs = Math.floor(Date.now() / 1000);
      const alignedNow = alignTimestampToInterval(nowTs, intervalSeconds);
      const steps = 96;
      debug.push(`Scan from ${alignedNow} back ${steps} steps @ ${intervalSeconds}s`);
      for (let step = 0; step < steps; step += 1) {
        const candidateTs = alignedNow - step * intervalSeconds;
        const candidateSlug = `${prefix}-${candidateTs}`;
        const candidate = await tryFetchTokenOptionsForSlug(candidateSlug);
        if (candidate.ok && candidate.options.length > 0) {
          const resolved = candidate.options.map((option) => option.tokenId);
          setLiveStreamSlug(candidateSlug);
          setLiveStreamTokenIds(resolved.join(', '));
          if (candidateSlug !== slug) {
            setLiveStreamWarning(`Resolved nearest market: ${candidateSlug}.`);
          }
          resolvedSlug = candidateSlug;
          debug.push(`Resolved: ${candidateSlug} (${resolved.length} tokens)`);
          setLiveStreamDebug(debug);
          return { tokens: resolved, slug: resolvedSlug };
        }
      }
    }
    const params = new URLSearchParams();
    params.set('prefix', prefix);
    const { response, payload } = await fetchContinuousJson<{
      market?: { slug?: string };
      slug?: string;
      options?: PriceHistoryOption[];
      error?: string;
    }>(`/api/markets/resolve-latest?${params.toString()}`);
    debug.push(
      `resolve-latest: ${response.status} ${response.ok ? 'ok' : 'fail'}`
    );
    if (payload.error) {
      debug.push(`resolve-latest error: ${payload.error}`);
    }
    if (response.ok) {
      const options = payload.options ?? [];
      if (options.length > 0) {
        const resolvedSlug = payload.market?.slug ?? payload.slug;
        if (resolvedSlug) {
          setLiveStreamSlug(resolvedSlug);
          if (resolvedSlug !== slug) {
            setLiveStreamWarning(
              `Resolved latest market for prefix ${prefix}: ${resolvedSlug}.`
            );
          }
        }
        const resolved = options.map((option) => option.tokenId);
        setLiveStreamTokenIds(resolved.join(', '));
        debug.push(`Resolved latest: ${resolvedSlug ?? prefix}`);
        setLiveStreamDebug(debug);
        return { tokens: resolved, slug: resolvedSlug ?? slug };
      }
    }

    const resolveParams = new URLSearchParams();
    resolveParams.set('prefix', prefix);
    resolveParams.set('limit', '200');
    const resolveResponse = await fetchContinuousJson<{
      results?: ResolvedMarket[];
      error?: string;
    }>(`/api/markets/resolve?${resolveParams.toString()}`);
    debug.push(
      `resolve list: ${resolveResponse.response.status} ${
        resolveResponse.response.ok ? 'ok' : 'fail'
      }`
    );
    if (resolveResponse.payload.error) {
      debug.push(`resolve list error: ${resolveResponse.payload.error}`);
    }
    if (resolveResponse.response.ok) {
      const resolvedLatest = pickLatestResolvedMarket(
        resolveResponse.payload.results ?? []
      );
      if (resolvedLatest) {
        if (resolvedLatest.slug !== slug) {
          setLiveStreamSlug(resolvedLatest.slug);
          setLiveStreamWarning(
            `Resolved latest market for prefix ${prefix}: ${resolvedLatest.slug}.`
          );
        }
        setLiveStreamTokenIds(resolvedLatest.tokens.join(', '));
        debug.push(`Resolved list latest: ${resolvedLatest.slug}`);
        setLiveStreamDebug(debug);
        return { tokens: resolvedLatest.tokens, slug: resolvedLatest.slug };
      }
    }

    setLiveStreamDebug(debug);
    throw new Error('No markets matched this prefix.');
  };

  const handleLiveStreamDisconnect = () => {
    liveStreamManualDisconnectRef.current = true;
    if (liveStreamSocketRef.current) {
      liveStreamSocketRef.current.close();
      liveStreamSocketRef.current = null;
    }
    liveStreamStateRef.current.connected = false;
    liveStreamStateRef.current.connecting = false;
    liveStreamStateRef.current.resolving = false;
    stopLiveStreamPolling();
    stopLiveStreamAdvance();
    clearLiveStreamSnapshots();
    setLiveStreamConnected(false);
    setLiveStreamConnecting(false);
  };

  const handleLiveStreamStop = () => {
    setLiveStreamAutoContinue(false);
    persistLiveStreamAutoState(false);
    handleLiveStreamDisconnect();
  };

  const handleLiveStreamConnect = async (
    _event?: MouseEvent<HTMLButtonElement> | null,
    options?: { slug?: string; tokenIds?: string[]; channels?: string[] }
  ) => {
    const state = liveStreamStateRef.current;
    if (state.connected || state.connecting || state.resolving) {
      return;
    }
    liveStreamManualDisconnectRef.current = false;
    const channels = options?.channels
      ? options.channels
      : Object.entries(liveStreamChannels)
          .filter(([, enabled]) => enabled)
          .map(([channel]) => channel);
    if (channels.length === 0) {
      setLiveStreamError('Select at least one channel.');
      return;
    }

    setLiveStreamError(null);
    setLiveStreamWarning(null);
    stopLiveStreamPolling();

    let tokens: string[] = [];
    let resolvedSlug = options?.slug?.trim() || liveStreamSlug.trim();
    liveStreamStateRef.current.resolving = true;
    setLiveStreamResolving(true);
    try {
      const resolved = await resolveLiveStreamTokens(
        options?.slug,
        options?.tokenIds
      );
      tokens = resolved.tokens;
      resolvedSlug = resolved.slug || resolvedSlug;
    } catch (error) {
      setLiveStreamError(
        error instanceof Error ? error.message : 'Unable to resolve token IDs.'
      );
      liveStreamStateRef.current.resolving = false;
      setLiveStreamResolving(false);
      return;
    }
    liveStreamStateRef.current.resolving = false;
    setLiveStreamResolving(false);

    handleLiveStreamClear();
    liveStreamStateRef.current.connecting = true;
    setLiveStreamConnecting(true);
    const marketId = registerLiveStreamMarket(
      resolvedSlug || liveStreamSlug.trim(),
      tokens
    );
    persistLiveStreamAutoState(
      true,
      resolvedSlug || liveStreamSlug.trim(),
      liveStreamTokenIds,
      getLiveStreamChannelList(liveStreamChannels)
    );
    setLiveStreamAutoContinue(true);

    const socket = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/');
    liveStreamSocketRef.current = socket;
    let socketOpened = false;

    socket.onopen = () => {
      socketOpened = true;
      liveStreamStateRef.current.connected = true;
      liveStreamStateRef.current.connecting = false;
      setLiveStreamConnected(true);
      setLiveStreamConnecting(false);
      stopLiveStreamPolling();
      channels.forEach((channel) => {
        socket.send(
          JSON.stringify({
            type: 'subscribe',
            event: 'subscribe',
            channel,
            tokens,
          })
        );
      });
    };

    socket.onmessage = (event) => {
      if (typeof event.data === 'string') {
        appendLiveStreamRaw(event.data, marketId);
      } else if (event.data instanceof Blob) {
        event.data
          .text()
          .then((raw) => appendLiveStreamRaw(raw, marketId))
          .catch(() => {});
      }
    };

    socket.onerror = () => {
      if (marketId !== liveStreamMarketIdRef.current) {
        return;
      }
      liveStreamStateRef.current.connected = false;
      liveStreamStateRef.current.connecting = false;
      setLiveStreamConnecting(false);
      setLiveStreamConnected(false);
      if (!liveStreamManualDisconnectRef.current) {
        startLiveStreamPolling(tokens, channels, 'WebSocket error.', marketId);
      } else {
        setLiveStreamError('WebSocket error. Check the connection.');
      }
    };

    socket.onclose = () => {
      if (marketId !== liveStreamMarketIdRef.current) {
        return;
      }
      liveStreamStateRef.current.connected = false;
      liveStreamStateRef.current.connecting = false;
      setLiveStreamConnected(false);
      setLiveStreamConnecting(false);
      if (
        !liveStreamManualDisconnectRef.current &&
        !liveStreamStateRef.current.polling
      ) {
        const reason = socketOpened
          ? 'WebSocket closed.'
          : 'WebSocket unavailable.';
        startLiveStreamPolling(tokens, channels, reason, marketId);
      }
    };
  };

  const handleMarketHistoryExport = async () => {
    const seriesId = marketHistorySeriesId.trim();
    if (!seriesId) {
      setMarketHistoryError('Enter a series ID.');
      return;
    }

    const startInput = marketHistoryStart.trim();
    const endInput = marketHistoryEnd.trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);

    if (startInput && startTs === null) {
      setMarketHistoryError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setMarketHistoryError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setMarketHistoryError('Start must be before end.');
      return;
    }

    const marketLimitInput = marketHistoryMarketLimit.trim();
    const marketLimitParsed = marketLimitInput ? parseInt(marketLimitInput, 10) : 200;
    if (!Number.isFinite(marketLimitParsed) || marketLimitParsed <= 0) {
      setMarketHistoryError('Max markets must be a positive number.');
      return;
    }

    const tradeLimitInput = marketHistoryTradeLimit.trim();
    const tradeLimitParsed = tradeLimitInput ? parseInt(tradeLimitInput, 10) : 500;
    if (!Number.isFinite(tradeLimitParsed) || tradeLimitParsed <= 0) {
      setMarketHistoryError('Trades per market must be a positive number.');
      return;
    }

    setMarketHistoryLoading(true);
    setMarketHistoryError(null);
    setMarketHistoryWarning(null);
    setMarketHistoryCancelRequested(false);
    marketHistoryCancelRef.current = false;
    setMarketHistoryProgress({ markets: 0, trades: 0, totalMarkets: 0 });

    let cancelled = false;
    try {
      const params = new URLSearchParams();
      params.set('seriesId', seriesId);
      params.set('limit', String(Math.min(marketLimitParsed, 2000)));
      if (startTs !== null) {
        params.set('startTs', String(startTs));
      }
      if (endTs !== null) {
        params.set('endTs', String(endTs));
      }

      const marketsResponse = await fetch(`/api/markets/series?${params.toString()}`);
      const marketsPayload = (await marketsResponse.json()) as {
        markets?: MarketSeriesEntry[];
        scanned?: number;
        truncated?: boolean;
        error?: string;
      };
      if (!marketsResponse.ok) {
        throw new Error(marketsPayload.error || 'Market lookup failed');
      }
      const markets = marketsPayload.markets ?? [];
      if (markets.length === 0) {
        setMarketHistoryError('No markets found for this series and range.');
        return;
      }
      if (marketsPayload.truncated) {
        setMarketHistoryWarning(
          'Markets list truncated. Increase Max markets if needed.'
        );
      }

      const trades: MarketTradeEntry[] = [];
      let missingCondition = 0;
      let marketsProcessed = 0;
      let tradesFetched = 0;
      setMarketHistoryProgress({ markets: 0, trades: 0, totalMarkets: markets.length });

      for (const market of markets) {
        if (marketHistoryCancelRef.current) {
          cancelled = true;
          break;
        }
        const conditionId = market.conditionId;
        if (!conditionId) {
          missingCondition += 1;
          marketsProcessed += 1;
          setMarketHistoryProgress((prev) => ({
            ...prev,
            markets: marketsProcessed,
          }));
          continue;
        }

        const tradeParams = new URLSearchParams();
        tradeParams.set('market', conditionId);
        tradeParams.set('limit', String(Math.min(tradeLimitParsed, 500)));
        if (startTs !== null) {
          tradeParams.set('startTs', String(startTs));
        }
        if (endTs !== null) {
          tradeParams.set('endTs', String(endTs));
        }

        try {
          const controller = new AbortController();
          marketHistoryAbortRef.current = controller;
          const tradeResponse = await fetch(
            `/api/markets/trades?${tradeParams.toString()}`,
            { signal: controller.signal }
          );
          const tradePayload = (await tradeResponse.json()) as {
            trades?: MarketTradeEntry[];
            error?: string;
          };
          if (!tradeResponse.ok) {
            throw new Error(tradePayload.error || 'Trade fetch failed');
          }
          const tradeRows = tradePayload.trades ?? [];
          trades.push(...tradeRows);
          tradesFetched += tradeRows.length;
        } catch (error) {
          if (error instanceof DOMException && error.name === 'AbortError') {
            if (marketHistoryCancelRef.current) {
              cancelled = true;
              break;
            }
          } else {
            setMarketHistoryWarning(
              (prev) =>
                prev
                  ? `${prev} Some markets failed to load trades.`
                  : 'Some markets failed to load trades.'
            );
          }
        } finally {
          marketsProcessed += 1;
          setMarketHistoryProgress({
            markets: marketsProcessed,
            trades: tradesFetched,
            totalMarkets: markets.length,
          });
        }
      }

      if (missingCondition > 0) {
        setMarketHistoryWarning(
          (prev) =>
            prev
              ? `${prev} ${missingCondition} markets missing condition IDs.`
              : `${missingCondition} markets missing condition IDs.`
        );
      }

      if (cancelled) {
        setMarketHistoryWarning(
          (prev) =>
            prev
              ? `${prev} Export stopped early; downloaded fetched trades only.`
              : 'Export stopped early; downloaded fetched trades only.'
        );
      }

      const workbook = buildMarketHistoryWorkbook(seriesId, markets, trades);
      const fileName = `market-history-series-${seriesId}-${new Date()
        .toISOString()
        .slice(0, 10)}.xlsx`;
      downloadWorkbook(workbook, fileName);
    } catch (error) {
      setMarketHistoryError(
        error instanceof Error ? error.message : 'Market history export failed'
      );
    } finally {
      marketHistoryAbortRef.current = null;
      marketHistoryCancelRef.current = false;
      setMarketHistoryLoading(false);
      setMarketHistoryCancelRequested(false);
    }
  };

  const handleRangeExportResume = () => {
    if (!rangeExportResumeCursor) {
      return;
    }
    handleRangeExport({
      start: rangeExportStart,
      end: String(rangeExportResumeCursor),
      limit: rangeExportLimit,
      initialWarning: `Resuming from ${rangeExportResumeCursor}.`,
      resumeCursor: rangeExportResumeCursor,
      resumeKeys: rangeExportResumeKeys,
    });
  };

  const handleFullExport = async () => {
    if (!trimmedWallet || !isValidAddress(trimmedWallet)) {
      setExportError('Enter a valid wallet address to export.');
      return;
    }
    if (exportLoading) {
      return;
    }

    setExportLoading(true);
    setExportError(null);
    setExportWarning(null);

    let trades: TradeActivity[] = [];
    let offset = 0;
    let truncated = false;
    let warning: string | undefined;
    const seenIds = new Set<string>();
    let lifetimeSummary: LifetimeSummary | null = null;
    let lifetimeError: string | undefined;

    try {
      while (true) {
        const response = await fetch(
          `/api/activity?user=${trimmedWallet}&limit=${EXPORT_BATCH_SIZE}&offset=${offset}`
        );
        if (!response.ok) {
          throw new Error('Export activity fetch failed');
        }
        const data = (await response.json()) as { trades?: TradeActivity[] };
        const batch = data.trades ?? [];
        if (batch.length === 0) {
          break;
        }
        const fresh = batch.filter((trade) => {
          const key = getTradeKey(trade);
          if (seenIds.has(key)) {
            return false;
          }
          seenIds.add(key);
          return true;
        });
        if (fresh.length === 0) {
          warning = 'Pagination returned duplicate results. Exported available trades.';
          break;
        }
        trades = trades.concat(fresh);
        offset += batch.length;
        if (batch.length < EXPORT_BATCH_SIZE) {
          break;
        }
        if (trades.length >= EXPORT_MAX_TRADES) {
          truncated = true;
          break;
        }
      }

      let summary: WalletSummary | null = null;
      let summaryError: string | undefined;
      try {
        const summaryResponse = await fetch(`/api/wallet-summary?user=${trimmedWallet}`);
        if (!summaryResponse.ok) {
          throw new Error('Wallet summary fetch failed');
        }
        const summaryData = (await summaryResponse.json()) as { summary?: WalletSummary };
        summary = summaryData.summary ?? null;
      } catch (error) {
        summaryError = error instanceof Error ? error.message : 'Wallet summary fetch failed';
      }

      try {
        const lifetimeResponse = await fetch(`/api/wallet-lifetime?user=${trimmedWallet}`);
        if (!lifetimeResponse.ok) {
          throw new Error('Lifetime summary fetch failed');
        }
        const lifetimeData = (await lifetimeResponse.json()) as { summary?: LifetimeSummary };
        lifetimeSummary = lifetimeData.summary ?? null;
      } catch (error) {
        lifetimeError = error instanceof Error ? error.message : 'Lifetime summary fetch failed';
      }

      const positionHistory = computePositionHistory(trades);
      const positionSummary = computePositionSummary(trades);
      const workbook = buildWalletExportWorkbook(
        trimmedWallet,
        summary,
        trades,
        summaryError,
        warning,
        truncated,
        positionHistory,
        positionSummary,
        lifetimeSummary,
        lifetimeError
      );
      const summarySheet = buildRangeSummarySheet(
        trimmedWallet,
        '',
        '',
        trades.length,
        positionSummary,
        computeTradeTotals(trades),
        0,
        1
      );
      XLSX.utils.book_append_sheet(workbook, summarySheet, 'Full Summary');
      const safeWallet = trimmedWallet.toLowerCase();
      const fileName = `wallet-history-${safeWallet.slice(0, 6)}${safeWallet.slice(-4)}-${new Date()
        .toISOString()
        .slice(0, 10)}.xlsx`;
      downloadWorkbook(workbook, fileName);
      if (warning) {
        setExportWarning(warning);
      }
    } catch (error) {
      setExportError(error instanceof Error ? error.message : 'Export failed');
    } finally {
      setExportLoading(false);
    }
  };

  const handleSnapshotExport = () => {
    if (!trimmedWallet || !isValidAddress(trimmedWallet)) {
      setExportError('Enter a valid wallet address to export.');
      return;
    }

    setExportError(null);
    setExportWarning(null);

    const positionHistory = computePositionHistory(manualTrades);
    const positionSummary = computePositionSummary(manualTrades);
    const workbook = buildWalletExportWorkbook(
      trimmedWallet,
      walletSummary,
      manualTrades,
      walletSummaryError ?? undefined,
      undefined,
      undefined,
      positionHistory,
      positionSummary,
      lifetimeSummary,
      lifetimeSummaryError ?? undefined
    );
    const safeWallet = trimmedWallet.toLowerCase();
    const fileName = `wallet-snapshot-${safeWallet.slice(0, 6)}${safeWallet.slice(-4)}-${new Date()
      .toISOString()
      .slice(0, 10)}.xlsx`;
    downloadWorkbook(workbook, fileName);
  };

  const handlePriceHistoryFetch = async () => {
    const prefixInput = priceHistoryPrefix.trim();
    let marketInput = priceHistoryMarket.trim();
    const tokenIds = parseTokenIdInput(priceHistoryTokenIds);
    const labelInput = parseOutcomeLabels(priceHistoryTokenLabels);
    const suggestedMarket =
      selectedMarket?.slug || selectedMarket?.clobTokenIds?.[0] || '';
    let market = marketInput || suggestedMarket;
    let resolvedOutcome = priceHistoryOutcome.trim();
    let resolvedOutcomeIndex = priceHistoryOutcomeIndex.trim();

    if (tokenIds.length > 0) {
      market = '';
    }

    if (prefixInput && tokenIds.length === 0) {
      try {
        const params = new URLSearchParams();
        params.set('prefix', prefixInput);
        const response = await fetch(`/api/markets/resolve-latest?${params.toString()}`);
        const payload = (await response.json()) as {
          error?: string;
          slug?: string;
          options?: PriceHistoryOption[];
        };
        if (!response.ok || !payload.slug) {
          throw new Error(payload.error || 'Resolve latest market failed');
        }
        market = payload.slug;
        setPriceHistoryMarket(payload.slug);
        if (!resolvedOutcome && !resolvedOutcomeIndex && payload.options?.length === 1) {
          const only = payload.options[0];
          resolvedOutcome = only.outcome ?? '';
          resolvedOutcomeIndex =
            only.outcomeIndex !== undefined && only.outcomeIndex !== null
              ? String(only.outcomeIndex)
              : '';
          setPriceHistoryOutcome(resolvedOutcome);
          setPriceHistoryOutcomeIndex(resolvedOutcomeIndex);
        }
      } catch (error) {
        setPriceHistoryError(
          error instanceof Error ? error.message : 'Resolve latest market failed'
        );
        return;
      }
    }

    if (!market && tokenIds.length === 0) {
      setPriceHistoryError('Enter a market slug, token id, or prefix.');
      return;
    }
    if (!marketInput && market && !prefixInput) {
      setPriceHistoryMarket(market);
    }

    if (resolvedOutcomeIndex && Number.isNaN(parseInt(resolvedOutcomeIndex, 10))) {
      setPriceHistoryError('Outcome index must be a number.');
      return;
    }

    const fidelityInput = priceHistoryFidelity.trim();
    if (fidelityInput && Number.isNaN(parseInt(fidelityInput, 10))) {
      setPriceHistoryError('Fidelity must be a number.');
      return;
    }

    const rowLimitInput = priceHistoryRowLimit.trim();
    let rowLimit: number | null = null;
    if (rowLimitInput) {
      const parsedRowLimit = parseInt(rowLimitInput, 10);
      if (!Number.isFinite(parsedRowLimit) || parsedRowLimit <= 0) {
        setPriceHistoryError('Row count must be a positive number.');
        return;
      }
      rowLimit = parsedRowLimit;
    }

    const startInput = priceHistoryStart.trim();
    const endInput = priceHistoryEnd.trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);

    if (startInput && startTs === null) {
      setPriceHistoryError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setPriceHistoryError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setPriceHistoryError('Start must be before end.');
      return;
    }

    const params = new URLSearchParams();
    params.set('market', market);

    if (resolvedOutcome) {
      params.set('outcome', resolvedOutcome);
    }
    if (resolvedOutcomeIndex) {
      params.set('outcomeIndex', resolvedOutcomeIndex);
    }
    if (startTs !== null) {
      params.set('startTs', String(startTs));
    }
    if (endTs !== null) {
      params.set('endTs', String(endTs));
    }
    if (fidelityInput) {
      params.set('fidelity', fidelityInput);
    }

    setPriceHistoryLoading(true);
    setPriceHistoryError(null);
    setPriceHistorySeries([]);
    setPriceHistoryActiveToken(null);
    setPriceHistoryOptions([]);

    try {
      if (tokenIds.length > 0) {
        const series: PriceHistorySeries[] = [];
        let failures = 0;
        const labels = inferTokenLabels(
          labelInput,
          tokenIds.length,
          marketInput,
          prefixInput
        );

        for (let index = 0; index < tokenIds.length; index += 1) {
          const tokenId = tokenIds[index];
          const tokenParams = new URLSearchParams();
          tokenParams.set('tokenId', tokenId);
          if (startTs !== null) {
            tokenParams.set('startTs', String(startTs));
          }
          if (endTs !== null) {
            tokenParams.set('endTs', String(endTs));
          }
          if (fidelityInput) {
            tokenParams.set('fidelity', fidelityInput);
          }

          try {
            const tokenResponse = await fetch(
              `/api/price-history?${tokenParams.toString()}`
            );
            const tokenPayload = (await tokenResponse.json()) as {
              history?: unknown;
              priceToBeat?: number | null;
              currentPrice?: number | null;
              liquidityClob?: number | null;
            };
            if (!tokenResponse.ok) {
              failures += 1;
              continue;
            }
            const points = extractPriceHistoryPoints(tokenPayload.history);
            const limited = limitPriceHistoryPoints(points, rowLimit);
            if (limited.length === 0) {
              continue;
            }
            series.push({
              tokenId,
              outcome: labels[index] ?? `Token ${index + 1}`,
              points: limited,
              priceToBeat: tokenPayload.priceToBeat ?? null,
              currentPrice: tokenPayload.currentPrice ?? null,
              liquidityClob: tokenPayload.liquidityClob ?? null,
            });
          } catch (_error) {
            failures += 1;
          }
        }

        if (series.length === 0) {
          throw new Error('No price history returned for this selection.');
        }

        setPriceHistoryOptions(
          series.map((entry, index) => ({
            tokenId: entry.tokenId,
            outcome: entry.outcome ?? labels[index] ?? `Token ${index + 1}`,
          }))
        );
        applyPriceHistorySeries(series, series[0].tokenId);
        if (failures > 0) {
          setPriceHistoryError(`Some tokens failed to load (${failures}).`);
        }
        return;
      }

      const response = await fetch(`/api/price-history?${params.toString()}`);
      const payload = (await response.json()) as {
        error?: string;
        tokenId?: string;
        slug?: string;
        outcome?: string;
        outcomeIndex?: number;
        history?: unknown;
        options?: PriceHistoryOption[];
        priceToBeat?: number | null;
        currentPrice?: number | null;
        liquidityClob?: number | null;
      };

      if (!response.ok) {
        if (payload.options && payload.options.length > 0) {
          const series: PriceHistorySeries[] = [];
          let failures = 0;

          for (const option of payload.options) {
            const optionParams = new URLSearchParams();
            optionParams.set('tokenId', option.tokenId);
            if (startTs !== null) {
              optionParams.set('startTs', String(startTs));
            }
            if (endTs !== null) {
              optionParams.set('endTs', String(endTs));
            }
            if (fidelityInput) {
              optionParams.set('fidelity', fidelityInput);
            }

            try {
              const optionResponse = await fetch(
                `/api/price-history?${optionParams.toString()}`
              );
              const optionPayload = (await optionResponse.json()) as {
                history?: unknown;
                tokenId?: string;
                priceToBeat?: number | null;
                currentPrice?: number | null;
                liquidityClob?: number | null;
              };
              if (!optionResponse.ok) {
                failures += 1;
                continue;
              }
              const points = extractPriceHistoryPoints(optionPayload.history);
              const limited = limitPriceHistoryPoints(points, rowLimit);
              if (limited.length === 0) {
                continue;
              }
              series.push({
                tokenId: option.tokenId,
                slug: market,
                outcome: option.outcome,
                outcomeIndex: option.outcomeIndex,
                points: limited,
                priceToBeat: optionPayload.priceToBeat ?? null,
                currentPrice: optionPayload.currentPrice ?? null,
                liquidityClob: optionPayload.liquidityClob ?? null,
              });
            } catch (_error) {
              failures += 1;
            }
          }

          if (series.length === 0) {
            throw new Error('No price history returned for this selection.');
          }

          setPriceHistoryOptions(payload.options);
          let preferredToken: string | null = null;
          if (resolvedOutcomeIndex) {
            const indexValue = parseInt(resolvedOutcomeIndex, 10);
            if (!Number.isNaN(indexValue)) {
              preferredToken =
                payload.options.find((option) => option.outcomeIndex === indexValue)
                  ?.tokenId ?? null;
            }
          }
          if (!preferredToken && resolvedOutcome) {
            const normalized = resolvedOutcome.trim().toLowerCase();
            preferredToken =
              payload.options.find(
                (option) => (option.outcome ?? '').toLowerCase() === normalized
              )?.tokenId ?? null;
          }
          applyPriceHistorySeries(series, preferredToken ?? series[0].tokenId);
          if (failures > 0) {
            setPriceHistoryError(`Some outcomes failed to load (${failures}).`);
          }
          return;
        }
        throw new Error(payload.error || 'Price history fetch failed');
      }

      const points = extractPriceHistoryPoints(payload.history);
      const limited = limitPriceHistoryPoints(points, rowLimit);
      if (payload.tokenId) {
        applyPriceHistorySeries(
          [
            {
              tokenId: payload.tokenId,
              slug: payload.slug,
              outcome: payload.outcome,
              outcomeIndex: payload.outcomeIndex,
              points: limited,
              priceToBeat: payload.priceToBeat ?? null,
              currentPrice: payload.currentPrice ?? null,
              liquidityClob: payload.liquidityClob ?? null,
            },
          ],
          payload.tokenId
        );
      } else {
        applyPriceHistorySeries([]);
      }
      if (points.length === 0) {
        setPriceHistoryError('No price history returned for this selection.');
      }
    } catch (error) {
      applyPriceHistorySeries([]);
      setPriceHistoryError(error instanceof Error ? error.message : 'Price history fetch failed');
    } finally {
      setPriceHistoryLoading(false);
    }
  };

  const handlePriceHistoryDownload = () => {
    if (priceHistoryPoints.length === 0) {
      return;
    }
    const csv = buildPriceHistorySeriesCsv(
      priceHistorySeries.length > 0
        ? priceHistorySeries
        : [
            {
              tokenId: priceHistoryMeta?.tokenId ?? 'market',
              slug: priceHistoryMeta?.slug,
              outcome: priceHistoryMeta?.outcome,
              outcomeIndex: priceHistoryMeta?.outcomeIndex,
              priceToBeat: priceHistoryMeta?.priceToBeat ?? null,
              currentPrice: priceHistoryMeta?.currentPrice ?? null,
              liquidityClob: priceHistoryMeta?.liquidityClob ?? null,
              points: priceHistoryPoints,
            },
          ]
    );
    const tokenIds = parseTokenIdInput(priceHistoryTokenIds);
    const tokenLabel = tokenIds.length > 0 ? formatTokenLabel(tokenIds[0]) : null;
    const resolvedSlug = priceHistoryMeta?.slug;
    const prefixLabel = priceHistoryPrefix.trim();
    const marketLabel =
      tokenIds.length > 0
        ? tokenLabel || 'tokens'
        : resolvedSlug || prefixLabel || priceHistoryMarket.trim() || 'market';
    const fileName = `price-history-${marketLabel}-${new Date().toISOString().slice(0, 10)}.csv`;
    downloadCsv(csv, fileName);
  };

  const handlePriceHistoryUseSelectedMarket = () => {
    if (!selectedMarket) {
      return;
    }
    const fallbackToken = selectedMarket.clobTokenIds?.[0];
    setPriceHistoryMarket(selectedMarket.slug || fallbackToken || '');
    setPriceHistoryPrefix('');
    setPriceHistoryError(null);
  };

  const handlePriceHistoryUseOption = (option: PriceHistoryOption) => {
    const existingSeries = priceHistorySeries.find(
      (entry) => entry.tokenId === option.tokenId
    );
    if (existingSeries) {
      setPriceHistoryOutcome(option.outcome ?? '');
      setPriceHistoryOutcomeIndex(
        option.outcomeIndex !== undefined && option.outcomeIndex !== null
          ? String(option.outcomeIndex)
          : ''
      );
      applyPriceHistorySeries(priceHistorySeries, option.tokenId);
      return;
    }
    setPriceHistoryMarket(option.tokenId);
    setPriceHistoryOutcome(option.outcome ?? '');
    setPriceHistoryOutcomeIndex(
      option.outcomeIndex !== undefined && option.outcomeIndex !== null
        ? String(option.outcomeIndex)
        : ''
    );
    setPriceHistoryPrefix('');
    setPriceHistoryOptions([]);
  };

  const fetchRangeExportEarliestDate = async () => {
    if (!trimmedWallet || !isValidAddress(trimmedWallet)) {
      return { error: 'Enter a valid wallet address first.' };
    }

    const response = await fetch(`/api/activity/earliest?user=${trimmedWallet}`);
    const payload = (await response.json()) as {
      error?: string;
      earliestTimestamp?: number;
      scanned?: number;
      truncated?: boolean;
    };
    if (!response.ok || !payload.earliestTimestamp) {
      return { error: payload.error || 'Earliest activity lookup failed' };
    }

    const date = new Date(payload.earliestTimestamp * 1000)
      .toISOString()
      .slice(0, 10);
    const warning = payload.truncated
      ? `Earliest scan hit the limit after ${payload.scanned ?? 0} trades.`
      : payload.scanned
        ? `Earliest trade found after scanning ${payload.scanned} trades.`
        : undefined;

    return { date, warning };
  };

  const handleRangeExportUseEarliest = async () => {
    setRangeExportEarliestLoading(true);
    setRangeExportEarliestError(null);

    try {
      const result = await fetchRangeExportEarliestDate();
      if (result.error) {
        throw new Error(result.error);
      }
      if (result.date) {
        setRangeExportStart(result.date);
      }
      if (result.warning) {
        setRangeExportWarning(result.warning);
      }
    } catch (error) {
      setRangeExportEarliestError(
        error instanceof Error ? error.message : 'Earliest activity lookup failed'
      );
    } finally {
      setRangeExportEarliestLoading(false);
    }
  };

  const handleRangeExportEverything = async () => {
    if (rangeExportLoading || rangeExportEarliestLoading) {
      return;
    }

    setRangeExportError(null);
    setRangeExportWarning(null);

    let earliestDate: string | undefined;
    let earliestWarning: string | undefined;

    if (!rangeExportStart.trim()) {
      setRangeExportEarliestLoading(true);
      setRangeExportEarliestError(null);
      try {
        const result = await fetchRangeExportEarliestDate();
        if (result.error) {
          throw new Error(result.error);
        }
        earliestDate = result.date;
        earliestWarning = result.warning;
        if (earliestDate) {
          setRangeExportStart(earliestDate);
        }
      } catch (error) {
        setRangeExportEarliestError(
          error instanceof Error ? error.message : 'Earliest activity lookup failed'
        );
        return;
      } finally {
        setRangeExportEarliestLoading(false);
      }
    }

    const startValue = earliestDate ?? rangeExportStart.trim();
    const limitValue = '1000000';
    setRangeExportEnd('');
    setRangeExportLimit(limitValue);

    await handleRangeExport({
      start: startValue,
      end: '',
      limit: limitValue,
      initialWarning: earliestWarning,
    });
  };

  const handleResolvePrefix = async () => {
    const prefixInput = resolvePrefix.trim();
    const suggestedPrefix = selectedMarket?.slug
      ? stripSlugTimestamp(selectedMarket.slug)
      : '';
    const prefix = prefixInput || suggestedPrefix;
    if (!prefix) {
      setResolveError('Enter a slug prefix.');
      return;
    }
    if (!prefixInput && prefix) {
      setResolvePrefix(prefix);
    }

    const startInput = resolveStart.trim();
    const endInput = resolveEnd.trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);

    if (startInput && startTs === null) {
      setResolveError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setResolveError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setResolveError('Start must be before end.');
      return;
    }

    const limitInput = resolveLimit.trim();
    let parsedLimit: number | null = null;
    if (limitInput) {
      const limitValue = parseInt(limitInput, 10);
      if (!Number.isFinite(limitValue) || limitValue <= 0) {
        setResolveError('Max markets must be a positive number.');
        return;
      }
      parsedLimit = limitValue;
    }
    const resolvedLimit =
      parsedLimit ?? estimateResolveLimit(prefix, startTs, endTs);

    const params = new URLSearchParams();
    params.set('prefix', prefix);
    params.set('limit', String(Math.min(resolvedLimit, MAX_RESOLVE_LIMIT)));
    if (startTs !== null) {
      params.set('startTs', String(startTs));
    }
    if (endTs !== null) {
      params.set('endTs', String(endTs));
    }

    setResolveLoading(true);
    setResolveError(null);
    setResolveWarning(null);
    setResolveResults([]);

    try {
      const response = await fetch(`/api/markets/resolve?${params.toString()}`);
      const payload = (await response.json()) as {
        error?: string;
        results?: ResolvedMarket[];
        truncated?: boolean;
        scanned?: number;
      };
      if (!response.ok) {
        throw new Error(payload.error || 'Resolve failed');
      }
      const results = payload.results ?? [];
      setResolveResults(results);
      if (payload.truncated) {
        setResolveWarning('Results truncated. Increase Max markets or scan limits.');
      } else if (results.length === 0) {
        setResolveWarning('No markets matched this prefix.');
      }
    } catch (error) {
      setResolveError(error instanceof Error ? error.message : 'Resolve failed');
    } finally {
      setResolveLoading(false);
    }
  };

  const handleResolveUseMarket = (entry: ResolvedMarket) => {
    setPriceHistoryMarket(entry.tokenId);
    setPriceHistoryOutcome(entry.outcome ?? '');
    setPriceHistoryOutcomeIndex(
      entry.outcomeIndex !== undefined && entry.outcomeIndex !== null
        ? String(entry.outcomeIndex)
        : ''
    );
    setPriceHistoryPrefix('');
  };

  const handleResolveDownload = async () => {
    if (resolveResults.length === 0) {
      setResolveError('Resolve markets first.');
      return;
    }

    const startInput = resolveStart.trim();
    const endInput = resolveEnd.trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);

    if (!startInput && !endInput) {
      setResolveError('Set a start or end time to download history.');
      return;
    }
    if (startInput && startTs === null) {
      setResolveError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setResolveError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setResolveError('Start must be before end.');
      return;
    }

    setResolveDownloadLoading(true);
    setResolveError(null);
    setResolveWarning(null);

    try {
      const rows: Array<Array<unknown>> = [
        [
          'slug',
          'tokenId',
          'outcome',
          'outcomeIndex',
          'timestamp',
          'datetime',
          'token_price',
          'price_to_beat',
          'current_price',
          'liquidity_clob',
        ],
      ];
      let failures = 0;

      for (const entry of resolveResults) {
        const params = new URLSearchParams();
        params.set('tokenId', entry.tokenId);
        if (startTs !== null) {
          params.set('startTs', String(startTs));
        }
        if (endTs !== null) {
          params.set('endTs', String(endTs));
        }

        try {
          const response = await fetch(`/api/price-history?${params.toString()}`);
          const payload = (await response.json()) as {
            error?: string;
            history?: unknown;
            priceToBeat?: number | null;
            currentPrice?: number | null;
            liquidityClob?: number | null;
          };
          if (!response.ok) {
            failures += 1;
            continue;
          }
          const points = extractPriceHistoryPoints(payload.history);
          const resolved = normalizePriceHistorySeriesMeta({
            tokenId: entry.tokenId,
            slug: entry.slug,
            outcome: entry.outcome,
            outcomeIndex: entry.outcomeIndex,
            points,
            priceToBeat: payload.priceToBeat ?? null,
            currentPrice: payload.currentPrice ?? null,
            liquidityClob: payload.liquidityClob ?? null,
          });
          points.forEach((point) => {
            rows.push([
              entry.slug,
              entry.tokenId,
              entry.outcome ?? '',
              entry.outcomeIndex ?? '',
              point.timestamp,
              new Date(point.timestamp * 1000).toISOString(),
              point.price,
              resolved.priceToBeat ?? '',
              resolved.currentPrice ?? '',
              resolved.liquidityClob ?? '',
            ]);
          });
        } catch (_error) {
          failures += 1;
        }
      }

      if (rows.length === 1) {
        setResolveError('No price history returned for this range.');
        return;
      }

      if (failures > 0) {
        setResolveWarning(`Some markets failed (${failures}). Exported available data.`);
      }

      const csv = rows.map((row) => row.map(escapeCsvValue).join(',')).join('\n');
      const fileName = `prefix-history-${resolvePrefix || 'markets'}-${new Date()
        .toISOString()
        .slice(0, 10)}.csv`;
      downloadCsv(csv, fileName);
    } finally {
      setResolveDownloadLoading(false);
    }
  };

  const handleRangeExport = async (
    eventOrOptions?:
      | MouseEvent<HTMLButtonElement>
      | {
          start?: string;
          end?: string;
          limit?: string;
          initialWarning?: string;
          resumeCursor?: number | null;
          resumeKeys?: string[];
        }
  ) => {
    const options =
      eventOrOptions && 'currentTarget' in eventOrOptions ? undefined : eventOrOptions;
    if (!trimmedWallet || !isValidAddress(trimmedWallet)) {
      setRangeExportError('Enter a valid wallet address to export.');
      return;
    }

    const startInput = (options?.start ?? rangeExportStart).trim();
    const endInput = (options?.end ?? rangeExportEnd).trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);
    const resumeCursor = options?.resumeCursor ?? null;
    const resumeKeys = options?.resumeKeys ?? [];
    const effectiveEndTs = resumeCursor ?? endTs;

    if (startInput && startTs === null) {
      setRangeExportError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setRangeExportError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setRangeExportError('Start must be before end.');
      return;
    }

    const limitInput = (options?.limit ?? rangeExportLimit).trim();
    const parsedLimit = limitInput ? parseInt(limitInput, 10) : 5000;
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      setRangeExportError('Max rows must be a positive number.');
      return;
    }

    setRangeExportLoading(true);
    setRangeExportError(null);
    setRangeExportWarning(options?.initialWarning ?? null);
    setRangeExportPreview([]);
    setRangeExportPreviewTotal(null);
    setRangeExportProgress(0);
    setRangeExportPages(0);
    setRangeExportCancelRequested(false);
    setRangeExportResumeCursor(null);
    setRangeExportResumeKeys([]);
    rangeExportCancelRef.current = false;

    try {
      const trades: TradeActivity[] = [];
      const seenIds = new Set<string>(resumeKeys);
      let duplicateCount = 0;
      let cursorEnd = effectiveEndTs;
      let exhausted = false;
      let cancelled = false;
      let safety = 0;
      let pages = 0;
      let lastBoundaryKeys: string[] = resumeKeys;
      const maxRows = Math.floor(parsedLimit);
      const requestCap = 500;
      const appendRangeWarning = (message: string) =>
        setRangeExportWarning((prev) => (prev ? `${prev} ${message}` : message));

      while (!exhausted && trades.length < maxRows) {
        if (rangeExportCancelRef.current) {
          cancelled = true;
          appendRangeWarning('Export canceled by user.');
          break;
        }

        const remaining = maxRows - trades.length;
        const limit = Math.min(requestCap, remaining);
        const params = new URLSearchParams();
        params.set('user', trimmedWallet);
        params.set('limit', String(limit));
        params.set('cursor', '1');
        if (startTs !== null) {
          params.set('startTs', String(startTs));
        }
        if (cursorEnd !== null) {
          params.set('endTs', String(cursorEnd));
        }

        let payload: {
          trades?: TradeActivity[];
          error?: string;
          nextEnd?: number | null;
          oldest?: number | null;
          exhausted?: boolean;
        };
        try {
          const controller = new AbortController();
          rangeExportAbortRef.current = controller;
          const response = await fetch(`/api/activity?${params.toString()}`, {
            signal: controller.signal,
          });
          payload = (await response.json()) as {
            trades?: TradeActivity[];
            error?: string;
            nextEnd?: number | null;
            oldest?: number | null;
            exhausted?: boolean;
          };
          if (!response.ok) {
            throw new Error(payload.error || 'Range export failed');
          }
        } catch (error) {
          if (error instanceof DOMException && error.name === 'AbortError') {
            if (rangeExportCancelRef.current) {
              cancelled = true;
              appendRangeWarning('Export canceled by user.');
              break;
            }
          }
          throw error;
        }

        const batch = payload.trades ?? [];
        if (payload.oldest !== null && payload.oldest !== undefined) {
          lastBoundaryKeys = batch
            .filter(
              (trade) => normalizeTimestampSeconds(trade.timestamp) === payload.oldest
            )
            .map((trade) => getTradeKey(trade));
        }
        const fresh = batch.filter((trade) => {
          const key = getTradeKey(trade);
          if (seenIds.has(key)) {
            duplicateCount += 1;
            return false;
          }
          seenIds.add(key);
          return true;
        });
        trades.push(...fresh);

        const nextEnd =
          typeof payload.nextEnd === 'number' ? payload.nextEnd : null;
        if (nextEnd === null) {
          exhausted = true;
          break;
        }
        if (cursorEnd !== null && nextEnd === cursorEnd && fresh.length === 0) {
          appendRangeWarning('Cursor stalled; exported available data.');
          break;
        }
        cursorEnd = nextEnd;
        exhausted = Boolean(payload.exhausted) || batch.length === 0;
        if (batch.length < limit && payload.exhausted === undefined) {
          exhausted = true;
        }

        safety += 1;
        if (safety > 2000) {
          appendRangeWarning('Pagination safety stop reached.');
          break;
        }

        pages += 1;
        setRangeExportProgress(trades.length);
        setRangeExportPages(pages);
      }

      if (trades.length === 0) {
        setRangeExportError(
          cancelled
            ? 'Export canceled before any trades were returned.'
            : 'No trades returned for this time range.'
        );
        return;
      }

      if (duplicateCount > 0) {
        appendRangeWarning(`Removed ${duplicateCount} duplicate trades across pages.`);
      }
      if (!exhausted && trades.length >= maxRows) {
        appendRangeWarning(
          `Export capped at ${maxRows} rows. Increase max rows to continue.`
        );
      }
      if (cancelled) {
        appendRangeWarning('Export stopped early; downloaded fetched trades only.');
      }

      setRangeExportPreviewTotal(trades.length);
      setRangeExportPreview(trades.slice(0, RANGE_PREVIEW_LIMIT));
      if (!exhausted && cursorEnd !== null) {
        setRangeExportResumeCursor(cursorEnd);
        setRangeExportResumeKeys(lastBoundaryKeys);
      }

    const rangeLabel =
      startTs || endTs
        ? `Range export: ${startInput || '-'} to ${endInput || '-'}`
        : 'Range export: latest trades';
    const chunks =
      trades.length > RANGE_EXPORT_CHUNK_SIZE
        ? Math.ceil(trades.length / RANGE_EXPORT_CHUNK_SIZE)
        : 1;
    const rangePositionSummary = computePositionSummary(trades);
    const rangeTradeTotals = computeTradeTotals(trades);
    for (let index = 0; index < chunks; index += 1) {
      const startIndex = index * RANGE_EXPORT_CHUNK_SIZE;
      const endIndex = Math.min(startIndex + RANGE_EXPORT_CHUNK_SIZE, trades.length);
      const chunkTrades = trades.slice(startIndex, endIndex);
      const chunkLabel =
          chunks > 1 ? `${rangeLabel} (part ${index + 1}/${chunks})` : rangeLabel;
        const positionHistory = computePositionHistory(chunkTrades);
        const positionSummary = computePositionSummary(chunkTrades);
        const workbook = buildWalletExportWorkbook(
          trimmedWallet,
          null,
          chunkTrades,
          undefined,
          chunkLabel,
          !exhausted && trades.length >= maxRows ? true : undefined,
          positionHistory,
          positionSummary,
          null,
          undefined
        );
        const summarySheet = buildRangeSummarySheet(
          trimmedWallet,
          startInput,
          endInput,
          trades.length,
          rangePositionSummary,
          rangeTradeTotals,
          index,
          chunks
        );
        XLSX.utils.book_append_sheet(workbook, summarySheet, 'Range Summary');
        const fileName = `wallet-activity-range-${trimmedWallet.slice(
          0,
          6
        )}${trimmedWallet.slice(-4)}-${new Date().toISOString().slice(0, 10)}${
          chunks > 1 ? `-part${index + 1}` : ''
        }.xlsx`;
        downloadWorkbook(workbook, fileName);
      }
    } catch (error) {
      setRangeExportError(error instanceof Error ? error.message : 'Range export failed');
    } finally {
      rangeExportAbortRef.current = null;
      rangeExportCancelRef.current = false;
      setRangeExportLoading(false);
      setRangeExportCancelRequested(false);
    }
  };

  const handleTraderProfileExport = async () => {
    const fallbackWallet = trimmedWallet;
    const wallet = traderProfileWallet.trim() || fallbackWallet;
    if (!wallet || !isValidAddress(wallet)) {
      setTraderProfileError('Enter a valid wallet address to export.');
      return;
    }

    const startInput = traderProfileStart.trim();
    const endInput = traderProfileEnd.trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);

    if (startInput && startTs === null) {
      setTraderProfileError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setTraderProfileError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setTraderProfileError('Start must be before end.');
      return;
    }

    const limitInput = traderProfileLimit.trim();
    const limitValue = limitInput ? parseInt(limitInput, 10) : 5000;
    if (!Number.isFinite(limitValue) || limitValue <= 0) {
      setTraderProfileError('Max trades must be a positive number.');
      return;
    }

    setTraderProfileLoading(true);
    setTraderProfileError(null);
    setTraderProfileWarning(null);

    try {
      const params = new URLSearchParams();
      params.set('user', wallet);
      params.set('limit', String(Math.floor(limitValue)));
      if (startTs !== null) {
        params.set('startTs', String(startTs));
      }
      if (endTs !== null) {
        params.set('endTs', String(endTs));
      }

      const response = await fetch(`/api/trader/profile?${params.toString()}`);
      const payload = await readJsonResponse<TraderProfileResponse & { error?: string }>(
        response
      );
      if (!response.ok) {
        throw new Error(payload.error || 'Trader profile fetch failed');
      }

      const trades = payload.trades ?? [];
      if (trades.length === 0) {
        setTraderProfileError('No trades returned for this wallet.');
        return;
      }

      if (trades.length >= limitValue) {
        setTraderProfileWarning(
          `Reached limit of ${limitValue} trades. Increase max trades to fetch more.`
        );
      }

      const stamp = new Date().toISOString().slice(0, 10);
      const walletLabel = `${wallet.slice(0, 6)}${wallet.slice(-4)}`;
      const baseName = `trader-${walletLabel}-${stamp}`;

      const summaryCsv = buildTraderSummaryCsv(wallet, payload.summary, payload.range);
      downloadCsv(summaryCsv, `${baseName}-summary.csv`);
      downloadCsv(buildTraderPositionsCsv(payload.positions ?? []), `${baseName}-positions.csv`);
      downloadCsv(
        buildTraderPositionHistoryCsv(payload.positionHistory ?? []),
        `${baseName}-position-history.csv`
      );
      downloadCsv(buildTraderTradesCsv(trades), `${baseName}-trades.csv`);
      downloadCsv(
        buildCategorySummaryCsv(payload.categories ?? []),
        `${baseName}-categories.csv`
      );
      downloadCsv(buildDailyPnlCsv(payload.dailyPnl ?? []), `${baseName}-daily-pnl.csv`);
    } catch (error) {
      setTraderProfileError(
        error instanceof Error ? error.message : 'Trader profile export failed'
      );
    } finally {
      setTraderProfileLoading(false);
    }
  };

  const handleTraderScan = async () => {
    const limitInput = traderScanLimit.trim();
    const limitValue = limitInput ? parseInt(limitInput, 10) : 20;
    if (!Number.isFinite(limitValue) || limitValue <= 0) {
      setTraderScanError('Top N must be a positive number.');
      return;
    }

    const marketLimitInput = traderScanMarketLimit.trim();
    const marketLimitValue = marketLimitInput ? parseInt(marketLimitInput, 10) : 25;
    if (!Number.isFinite(marketLimitValue) || marketLimitValue <= 0) {
      setTraderScanError('Markets scanned must be a positive number.');
      return;
    }

    const tradeLimitInput = traderScanTradeLimit.trim();
    const tradeLimitValue = tradeLimitInput ? parseInt(tradeLimitInput, 10) : 200;
    if (!Number.isFinite(tradeLimitValue) || tradeLimitValue <= 0) {
      setTraderScanError('Trades per market must be a positive number.');
      return;
    }

    const startInput = traderScanStart.trim();
    const endInput = traderScanEnd.trim();
    const startTs = parseTimestampInput(startInput);
    const endTs = parseTimestampInput(endInput);

    if (startInput && startTs === null) {
      setTraderScanError('Invalid start timestamp.');
      return;
    }
    if (endInput && endTs === null) {
      setTraderScanError('Invalid end timestamp.');
      return;
    }
    if (startTs !== null && endTs !== null && startTs > endTs) {
      setTraderScanError('Start must be before end.');
      return;
    }

    setTraderScanLoading(true);
    setTraderScanError(null);
    setTraderScanResults([]);
    setTraderScanScannedMarkets(null);

    try {
      const params = new URLSearchParams();
      params.set('limit', String(Math.floor(limitValue)));
      params.set('marketLimit', String(Math.floor(marketLimitValue)));
      params.set('tradeLimit', String(Math.floor(tradeLimitValue)));
      params.set('status', traderScanStatus);
      params.set('sort', traderScanSort);
      if (startTs !== null) {
        params.set('startTs', String(startTs));
      }
      if (endTs !== null) {
        params.set('endTs', String(endTs));
      }

      const response = await fetch(`/api/traders/scan?${params.toString()}`);
      const payload = await readJsonResponse<{
        traders?: TraderScanEntry[];
        scannedMarkets?: number;
        error?: string;
      }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Trader scan failed');
      }

      const traders = payload.traders ?? [];
      setTraderScanResults(traders);
      setTraderScanScannedMarkets(payload.scannedMarkets ?? null);
      if (traders.length === 0) {
        setTraderScanError('No traders returned for this scan.');
      }
    } catch (error) {
      setTraderScanError(error instanceof Error ? error.message : 'Trader scan failed');
    } finally {
      setTraderScanLoading(false);
    }
  };

  const handleTraderScanExport = () => {
    if (traderScanResults.length === 0) {
      setTraderScanError('Scan results are empty.');
      return;
    }
    const stamp = new Date().toISOString().slice(0, 10);
    const csv = buildTraderScanCsv(traderScanResults);
    const fileName = `trader-scan-${traderScanStatus}-${stamp}.csv`;
    downloadCsv(csv, fileName);
  };

  const handleMarketExport = async () => {
    const limitInput = marketSearchLimit.trim();
    const limitValue = limitInput ? parseInt(limitInput, 10) : 50;
    if (limitInput && (!Number.isFinite(limitValue) || limitValue <= 0)) {
      setMarketExportError('Market limit must be a positive number.');
      return;
    }

    setMarketExportLoading(true);
    setMarketExportError(null);

    try {
      const params = new URLSearchParams();
      if (searchQuery.trim()) {
        params.set('query', searchQuery.trim());
      }
      params.set(
        'limit',
        String(
          Number.isFinite(limitValue) ? Math.min(Math.max(limitValue, 1), 500) : 50
        )
      );
      params.set('status', marketSearchStatus);

      const response = await fetch(`/api/markets/search?${params.toString()}`);
      const payload = await readJsonResponse<{
        markets?: Market[];
        error?: string;
      }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Market export failed');
      }

      const list = payload.markets ?? [];
      if (list.length === 0) {
        setMarketExportError('No markets returned for this search.');
        return;
      }

      const csv = buildMarketSearchCsv(list);
      const stamp = new Date().toISOString().slice(0, 10);
      const queryLabelRaw = searchQuery.trim()
        ? searchQuery
            .trim()
            .slice(0, 24)
            .replace(/[^a-zA-Z0-9-_]+/g, '-')
            .replace(/-+/g, '-')
            .replace(/^-|-$/g, '')
        : '';
      const queryLabel = queryLabelRaw || 'all';
      const fileName = `market-search-${marketSearchStatus}-${queryLabel}-${stamp}.csv`;
      downloadCsv(csv, fileName);
    } catch (error) {
      setMarketExportError(error instanceof Error ? error.message : 'Market export failed');
    } finally {
      setMarketExportLoading(false);
    }
  };

  const monitorSubtitle = hasValidWallet
    ? 'Live Polymarket activity for this wallet (refreshes every 10s)'
    : 'Enter a wallet address to load live + historical data';

  const monitorNote = manualError
    ? manualError
    : hasValidWallet
      ? 'Showing the latest 20 wallet trades.'
      : 'Paste any Polygon wallet to start analysis.';
  const exportNote = exportError
    ? exportError
    : exportWarning
      ? exportWarning
      : exportLoading
        ? 'Preparing export...'
        : null;
  const exportDisabled = exportLoading || !hasValidWallet;
  const resolvedTraderProfileWallet = traderProfileWallet.trim() || trimmedWallet;
  const hasTraderProfileWallet =
    Boolean(resolvedTraderProfileWallet) && isValidAddress(resolvedTraderProfileWallet);
  const snapshotNote = manualTrades.length
    ? `Snapshot includes the latest ${manualTrades.length} trades loaded.`
    : 'Snapshot will include the latest trades once activity loads.';
  const priceHistoryStats = useMemo(() => {
    if (priceHistoryPoints.length === 0) {
      return null;
    }
    const first = priceHistoryPoints[0];
    const last = priceHistoryPoints[priceHistoryPoints.length - 1];
    let minPrice = first.price;
    let maxPrice = first.price;
    priceHistoryPoints.forEach((point) => {
      if (point.price < minPrice) {
        minPrice = point.price;
      }
      if (point.price > maxPrice) {
        maxPrice = point.price;
      }
    });
    return {
      count: priceHistoryPoints.length,
      first,
      last,
      minPrice,
      maxPrice,
    };
  }, [priceHistoryPoints]);
  const priceHistoryChart = useMemo(() => {
    const sampled = downsamplePriceHistory(priceHistoryPoints, 160);
    return buildPriceHistoryChart(sampled);
  }, [priceHistoryPoints]);
  const priceHistoryLatest = useMemo(
    () => priceHistoryPoints.slice(-8).reverse(),
    [priceHistoryPoints]
  );
  const priceHistoryTableRows = useMemo(
    () => priceHistoryPoints.slice(-PRICE_HISTORY_TABLE_LIMIT),
    [priceHistoryPoints]
  );
  const marketMax = activitySummary.topMarkets.reduce(
    (max, entry) => (entry.usdc > max ? entry.usdc : max),
    0
  );
  const dailyNetLatest =
    dailyNetSeries.recent.length > 0
      ? dailyNetSeries.recent[dailyNetSeries.recent.length - 1].net
      : null;
  const hasTrades = manualTrades.length > 0;
  const hasLifetime = dailyNetSeries.recent.length > 0;
  const hasDailyTrend = dailyNetSeries.recent.length > 1;
  const winLossTotal = winLossDays.total || 1;
  const winPercent = (winLossDays.wins / winLossTotal) * 100;
  const lossPercent = (winLossDays.losses / winLossTotal) * 100;
  const flatPercent = (winLossDays.flat / winLossTotal) * 100;

  const summaryPnlClass =
    walletSummary && walletSummary.pnlPercent >= 0 ? 'price-up' : 'price-down';
  const lifetimeProfit = lifetimeSummary?.totalProfit ?? null;
  const lifetimeLoss = lifetimeSummary?.totalLoss ?? null;
  const lifetimeNet = lifetimeSummary?.netPnl ?? null;
  const openPnl =
    walletSummary && Number.isFinite(walletSummary.positionsValue)
      ? walletSummary.positionsValue - walletSummary.totalInitialValue
      : null;
  const lifetimeMarkToMarket =
    lifetimeSummary && walletSummary
      ? lifetimeSummary.netPnl + walletSummary.positionsValue
      : null;
  const lifetimeDayNet = lifetimeSummary?.lastDayNet ?? null;
  const lifetimeWinRate = lifetimeSummary?.winRate ?? null;
  const lifetimeProfitFactor = lifetimeSummary?.profitFactor ?? null;
  const lifetimeAvgWin = lifetimeSummary?.avgWinDay ?? null;
  const lifetimeAvgLoss = lifetimeSummary?.avgLossDay ?? null;
  const lifetimeMaxDrawdown = lifetimeSummary?.maxDrawdown ?? null;
  const lifetimeSharpe = lifetimeSummary?.sharpeRatio ?? null;
  const lifetimeActiveDays = lifetimeSummary?.activeDays ?? null;
  const lifetimeNote = lifetimeSummaryError
    ? lifetimeSummaryError
    : lifetimeSummary?.truncated
      ? `Lifetime stats truncated at ${lifetimeSummary.eventsCount} events. Increase LIFETIME_MAX_EVENTS to load more.`
      : lifetimeSummary?.firstTradeAt
        ? `Lifetime stats since ${formatTimestamp(lifetimeSummary.firstTradeAt)}.`
        : null;
  const lifetimeDetailNote =
    lifetimeSummary && !lifetimeSummaryError
      ? 'Lifetime PnL (cashflow) is net inflows minus outflows. MTM adds open positions value.'
      : null;
  if (!isAuthed) {
    return (
      <div className="login-page">
        <div className="login-card">
          <div className="login-header">
            <div className="logo-badge">P</div>
            <div>
              <p className="login-title">Polymarket Analysis Hub</p>
              <p className="login-subtitle">Sign in or create an account to access analytics</p>
            </div>
          </div>

          <form className="login-form" onSubmit={handleAuthSubmit}>
            <label className="login-label" htmlFor="auth-email">
              Email Address
            </label>
            <input
              id="auth-email"
              className="login-input"
              type="email"
              placeholder="you@company.com"
              autoComplete="email"
              value={authEmail}
              onChange={(event) => {
                setAuthEmail(event.target.value);
                if (authError) {
                  setAuthError(null);
                }
              }}
            />
            <label className="login-label" htmlFor="auth-password">
              Password
            </label>
            <input
              id="auth-password"
              className="login-input"
              type="password"
              placeholder="At least 8 characters"
              autoComplete={authMode === 'signup' ? 'new-password' : 'current-password'}
              value={authPassword}
              onChange={(event) => {
                setAuthPassword(event.target.value);
                if (authError) {
                  setAuthError(null);
                }
              }}
            />
            {authError ? <div className="login-error">{authError}</div> : null}
            <button className="login-button" type="submit" disabled={authLoading}>
              {authLoading
                ? authMode === 'signup'
                  ? 'Creating account...'
                  : 'Signing in...'
                : authMode === 'signup'
                  ? 'Create account'
                  : 'Sign in'}
            </button>
          </form>
          <div className="auth-switch">
            <span>
              {authMode === 'signup' ? 'Already have an account?' : 'New to the dashboard?'}
            </span>
            <button
              className="text-button"
              type="button"
              onClick={() => {
                setAuthMode(authMode === 'signup' ? 'login' : 'signup');
                setAuthError(null);
              }}
            >
              {authMode === 'signup' ? 'Sign in instead' : 'Create an account'}
            </button>
          </div>
          <p className="field-note">
            {RECAPTCHA_ENABLED && RECAPTCHA_SITE_KEY
              ? 'Protected by reCAPTCHA v3.'
              : 'reCAPTCHA is temporarily disabled.'}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <div className="logo-badge">P</div>
          <div>
            <p className="brand-name">Polymarket Data Lab</p>
            <p className="brand-subtitle">Wallet analytics and data extraction</p>
          </div>
        </div>
        <div className="topbar-metrics">
          {hasValidWallet && walletSummary ? (
            <>
              <div className="metric-card">
                <p className="metric-label">Portfolio</p>
                <p className="metric-value">{formatUsd(walletSummary.portfolioValue)}</p>
              </div>
              <div className="metric-card">
                <p className="metric-label">Lifetime Net</p>
                <p
                  className={`metric-value ${
                    lifetimeNet !== null && lifetimeNet < 0 ? 'price-down' : 'price-up'
                  }`}
                >
                  {lifetimeNet !== null && lifetimeNet > 0 ? '+' : ''}
                  {formatUsdOptional(lifetimeNet)}
                </p>
              </div>
            </>
          ) : walletSummaryError ? (
            <div className="metric-card">
              <p className="metric-label">Wallet</p>
              <p className="metric-value">Unavailable</p>
            </div>
          ) : (
            <div className="metric-card">
              <p className="metric-label">Status</p>
              <p className="metric-value">Awaiting wallet</p>
            </div>
          )}
          {authUser ? (
            <div className="metric-card account-card">
              <p className="metric-label">Account</p>
              <p className="metric-value">{authUser.email}</p>
              <p className="metric-sub">{authUser.role === 'admin' ? 'Admin' : 'Member'}</p>
            </div>
          ) : null}
        </div>
        <div className="actions">
          <span className={`status-pill status-${statusState}`}>{statusLabel}</span>
          {authError ? <span className="pill danger">{authError}</span> : null}
          <button
            className="ghost-button theme-toggle"
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
          >
            {theme === 'dark' ? 'Light mode' : 'Dark mode'}
          </button>
          <button className="ghost-button" onClick={handleLogout}>
            Logout
          </button>
        </div>
      </header>

      <nav className="page-nav" aria-label="Primary">
        <button
          className={`chip chip-button ${activePage === 'overview' ? 'is-active' : ''}`}
          type="button"
          onClick={() => setActivePage('overview')}
        >
          Overview
        </button>
        <button
          className={`chip chip-button ${activePage === 'trader' ? 'is-active' : ''}`}
          type="button"
          onClick={() => setActivePage('trader')}
        >
          Trader
        </button>
        <button
          className={`chip chip-button ${activePage === 'market' ? 'is-active' : ''}`}
          type="button"
          onClick={() => setActivePage('market')}
        >
          Market
        </button>
      </nav>

      {authUser && !authUser.walletAddress ? (
        <section className="panel onboarding" aria-label="Wallet onboarding">
          <div className="panel-header">
            <div>
              <p className="panel-title">Enter your wallet address</p>
              <p className="panel-subtitle">
                Save a default wallet to load live and historical analytics automatically.
              </p>
            </div>
            <span className="pill soft">Step 1</span>
          </div>
          <div className="onboarding-body">
            <input
              className="text-input"
              placeholder="0xYourWalletAddress"
              aria-label="Default wallet address"
              value={walletEntry}
              onChange={(event) => {
                setWalletEntry(event.target.value);
                if (walletSaveError) {
                  setWalletSaveError(null);
                }
              }}
            />
            <button
              className="primary-button"
              onClick={() => handleWalletSave()}
              disabled={walletSaveLoading}
            >
              {walletSaveLoading ? 'Saving...' : 'Save wallet'}
            </button>
          </div>
          {walletSaveError ? <p className="field-note error">{walletSaveError}</p> : null}
        </section>
      ) : null}

      <main className={`grid page-${activePage}`}>
        {activePage === 'overview' ? (
          <>
            <section className="panel monitor" aria-label="Wallet activity">
          <div className="panel-header">
            <div>
              <p className="panel-title">Wallet Activity</p>
              <p className="panel-subtitle">{monitorSubtitle}</p>
            </div>
            <div className="panel-actions">
              <button className="clear-button" onClick={() => setMonitorInput('')}>
                Clear
              </button>
            </div>
          </div>

          <div className="monitor-input">
            <div className="input-stack">
              <label className="field-label" htmlFor="wallet-address">
                Wallet Address
              </label>
              <input
                id="wallet-address"
                className="text-input"
                placeholder="0xYourWalletAddress"
                aria-label="Wallet address"
                value={monitorInput}
                onChange={(event) => {
                  setMonitorInput(event.target.value);
                  if (walletSaveError) {
                    setWalletSaveError(null);
                  }
                }}
              />
              {authUser ? (
                <div className="wallet-actions">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => handleWalletSave(monitorInput)}
                    disabled={walletSaveLoading}
                  >
                    {walletSaveLoading ? 'Saving...' : 'Save as default'}
                  </button>
                  <span className="field-note">
                    {authUser.walletAddress
                      ? `Default: ${formatAddress(authUser.walletAddress)}`
                      : 'No default wallet saved yet.'}
                  </span>
                </div>
              ) : null}
              {walletSaveError ? <span className="field-note error">{walletSaveError}</span> : null}
            </div>
          </div>

          {monitorNote ? <p className="field-note">{monitorNote}</p> : null}

          <div className="trade-list">
            {monitorRows.length === 0 ? (
              <div className="empty-state">No trades for this address yet.</div>
            ) : (
              monitorRows.map((order) => (
                <div className="trade-row" key={order.id}>
                  <div>
                    <p className="trade-market">{order.market}</p>
                    <p className="trade-time">{order.time}</p>
                  </div>
                  <div className="trade-meta">
                    <span className="chip">{order.wallet}</span>
                    <span className="value">{order.value}</span>
                    <span className={`pill ${order.side === 'buy' ? 'pill-buy' : 'pill-sell'}`}>
                      {order.side.toUpperCase()}
                    </span>
                  </div>
                </div>
              ))
            )}
          </div>

          <div className="activity-summary">
            <div className="activity-card">
              <p className="field-label">Buys</p>
              <p className="summary-value">{activitySummary.buyCount}</p>
              <p className="activity-sub">{formatUsd(activitySummary.buyUsd)}</p>
            </div>
            <div className="activity-card">
              <p className="field-label">Sells</p>
              <p className="summary-value">{activitySummary.sellCount}</p>
              <p className="activity-sub">{formatUsd(activitySummary.sellUsd)}</p>
            </div>
            <div className="activity-card">
              <p className="field-label">Net Flow</p>
              <p
                className={`summary-value ${
                  activitySummary.netUsd < 0 ? 'price-down' : 'price-up'
                }`}
              >
                {activitySummary.netUsd > 0 ? '+' : activitySummary.netUsd < 0 ? '-' : ''}
                {formatUsd(Math.abs(activitySummary.netUsd))}
              </p>
              <p className="activity-sub">USDC net</p>
            </div>
            <div className="activity-card">
              <p className="field-label">Last Activity</p>
              <p className="summary-value">
                {activitySummary.lastTimestamp
                  ? formatTimestamp(activitySummary.lastTimestamp)
                  : '--'}
              </p>
              <p className="activity-sub">Latest trade seen</p>
            </div>
          </div>

          <div className="market-breakdown">
            <p className="field-label">Top Markets (by USDC)</p>
            {activitySummary.topMarkets.length === 0 ? (
              <div className="empty-state">No market breakdown yet.</div>
            ) : (
              <div className="breakdown-list">
                {activitySummary.topMarkets.map((entry) => (
                  <div className="breakdown-row" key={entry.market}>
                    <span className="breakdown-name">{entry.market}</span>
                    <span className="breakdown-value">{formatUsd(entry.usdc)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
            </section>

            <div className="panel-stack overview-stack" aria-label="Wallet summary">
          <section className="panel summary" aria-label="Wallet summary">
            <div className="panel-header">
              <div>
                <p className="panel-title">Wallet Summary</p>
                <p className="panel-subtitle">Balance and PnL for the monitored wallet</p>
              </div>
              {walletSummary ? (
                <span className={`pill ${summaryPnlClass}`}>
                  {walletSummary.pnlPercent >= 0 ? '+' : ''}
                  {walletSummary.pnlPercent.toFixed(2)}%
                </span>
              ) : null}
            </div>

            {!hasValidWallet ? (
              <div className="empty-state">Enter a wallet address to see summary stats.</div>
            ) : walletSummaryLoading ? (
              <div className="empty-state">Loading wallet summary...</div>
            ) : walletSummaryError ? (
              <div className="empty-state">{walletSummaryError}</div>
            ) : walletSummary ? (
              <div className="summary-grid">
                <div>
                  <p className="field-label">USDC Balance</p>
                  <p className="summary-value">{formatUsd(walletSummary.balance)}</p>
                </div>
                <div>
                  <p className="field-label">Positions Value</p>
                  <p className="summary-value">{formatUsd(walletSummary.positionsValue)}</p>
                </div>
                <div>
                  <p className="field-label">Portfolio Value</p>
                  <p className="summary-value">{formatUsd(walletSummary.portfolioValue)}</p>
                </div>
                <div>
                  <p className="field-label">Open PnL (real-time)</p>
                  <p
                    className={`summary-value ${
                      openPnl !== null && openPnl < 0 ? 'price-down' : 'price-up'
                    }`}
                  >
                    {openPnl !== null && openPnl > 0 ? '+' : ''}
                    {formatUsdOptional(openPnl)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Lifetime PnL (cashflow)</p>
                  <p
                    className={`summary-value ${
                      lifetimeNet !== null && lifetimeNet < 0 ? 'price-down' : 'price-up'
                    }`}
                  >
                    {lifetimeNet !== null && lifetimeNet > 0 ? '+' : ''}
                    {lifetimeSummaryLoading ? 'Loading...' : formatUsdOptional(lifetimeNet)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Lifetime Profit</p>
                  <p className={`summary-value ${lifetimeProfit ? 'price-up' : ''}`}>
                    {lifetimeProfit && lifetimeProfit > 0 ? '+' : ''}
                    {lifetimeSummaryLoading ? 'Loading...' : formatUsdOptional(lifetimeProfit)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Lifetime Loss</p>
                  <p className={`summary-value ${lifetimeLoss ? 'price-down' : ''}`}>
                    {lifetimeLoss && lifetimeLoss > 0 ? '-' : ''}
                    {lifetimeSummaryLoading ? 'Loading...' : formatUsdOptional(lifetimeLoss)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Open Positions</p>
                  <p className="summary-value">{walletSummary.positionsCount}</p>
                </div>
                <div>
                  <p className="field-label">Wallet</p>
                  <p className="summary-value">{formatAddress(walletSummary.wallet)}</p>
                </div>
              </div>
            ) : (
              <div className="empty-state">Enter a wallet address to load stats.</div>
            )}
            {hasValidWallet && lifetimeNote ? <p className="field-note">{lifetimeNote}</p> : null}
            {hasValidWallet && lifetimeDetailNote ? (
              <p className="field-note">{lifetimeDetailNote}</p>
            ) : null}
          </section>

          <section className="panel analytics" aria-label="Trader analytics">
            <div className="panel-header">
              <div>
              <p className="panel-title">Lifetime Analytics</p>
              <p className="panel-subtitle">All-time performance from activity feed</p>
              </div>
              <span className="pill soft">All-time</span>
            </div>

            {!hasValidWallet ? (
              <div className="empty-state">Enter a wallet address to see analytics.</div>
            ) : lifetimeSummaryLoading ? (
              <div className="empty-state">Loading analytics...</div>
            ) : lifetimeSummaryError ? (
              <div className="empty-state">{lifetimeSummaryError}</div>
            ) : lifetimeSummary ? (
              <div className="summary-grid">
                <div>
                  <p className="field-label">All-time PnL</p>
                  <p
                    className={`summary-value ${
                      lifetimeNet !== null && lifetimeNet < 0 ? 'price-down' : 'price-up'
                    }`}
                  >
                    {lifetimeNet !== null && lifetimeNet > 0 ? '+' : ''}
                    {formatUsdOptional(lifetimeNet)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Lifetime PnL (MTM)</p>
                  <p
                    className={`summary-value ${
                      lifetimeMarkToMarket !== null && lifetimeMarkToMarket < 0
                        ? 'price-down'
                        : 'price-up'
                    }`}
                  >
                    {lifetimeMarkToMarket !== null && lifetimeMarkToMarket > 0 ? '+' : ''}
                    {formatUsdOptional(lifetimeMarkToMarket)}
                  </p>
                </div>
                <div>
                  <p className="field-label">24h PnL</p>
                  <p
                    className={`summary-value ${
                      lifetimeDayNet !== null && lifetimeDayNet < 0 ? 'price-down' : 'price-up'
                    }`}
                  >
                    {lifetimeDayNet !== null && lifetimeDayNet > 0 ? '+' : ''}
                    {formatUsdOptional(lifetimeDayNet)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Win Rate</p>
                  <p className="summary-value">{formatPercent(lifetimeWinRate)}</p>
                </div>
                <div>
                  <p className="field-label">Profit Factor</p>
                  <p className="summary-value">{formatRatio(lifetimeProfitFactor)}</p>
                </div>
                <div>
                  <p className="field-label">Avg Win Day</p>
                  <p className="summary-value">{formatUsdOptional(lifetimeAvgWin)}</p>
                </div>
                <div>
                  <p className="field-label">Avg Loss Day</p>
                  <p className="summary-value">{formatUsdOptional(lifetimeAvgLoss)}</p>
                </div>
                <div>
                  <p className="field-label">Max Drawdown</p>
                  <p className={`summary-value ${lifetimeMaxDrawdown ? 'price-down' : ''}`}>
                    {formatUsdOptional(lifetimeMaxDrawdown)}
                  </p>
                </div>
                <div>
                  <p className="field-label">Sharpe Ratio</p>
                  <p className="summary-value">{formatRatio(lifetimeSharpe)}</p>
                </div>
                <div>
                  <p className="field-label">Active Days</p>
                  <p className="summary-value">
                    {lifetimeActiveDays !== null && lifetimeActiveDays !== undefined
                      ? lifetimeActiveDays
                      : '--'}
                  </p>
                </div>
              </div>
            ) : (
              <div className="empty-state">Enter a wallet address to load analytics.</div>
            )}
          </section>

          <section className="panel alltime" aria-label="All-time statistics">
            <div className="panel-header">
              <div>
                <p className="panel-title">All-Time Statistics</p>
                <p className="panel-subtitle">High level performance snapshot</p>
              </div>
              <span className="pill soft">All-time</span>
            </div>

            {!hasValidWallet ? (
              <div className="empty-state">Enter a wallet address to load stats.</div>
            ) : lifetimeSummaryLoading ? (
              <div className="empty-state">Loading all-time stats...</div>
            ) : lifetimeSummaryError ? (
              <div className="empty-state">{lifetimeSummaryError}</div>
            ) : lifetimeSummary ? (
              <div className="alltime-content">
                <div className="alltime-grid">
                  <div className="stat-card">
                    <p className="stat-label">Total Trades</p>
                    <p className="stat-value">{lifetimeSummary.tradesCount}</p>
                  </div>
                  <div className="stat-card">
                    <p className="stat-label">Win Rate</p>
                    <p className="stat-value">{formatPercent(lifetimeWinRate)}</p>
                  </div>
                  <div className="stat-card">
                    <p className="stat-label">Net PnL</p>
                    <p
                      className={`stat-value ${
                        lifetimeNet !== null && lifetimeNet < 0 ? 'price-down' : 'price-up'
                      }`}
                    >
                      {lifetimeNet !== null && lifetimeNet > 0 ? '+' : ''}
                      {formatUsdOptional(lifetimeNet)}
                    </p>
                  </div>
                </div>

                <div className="alltime-account">
                  <div>
                    <p className="field-label">Member Since</p>
                    <p className="summary-value">
                      {authUser?.createdAt ? formatDateLabel(authUser.createdAt) : '--'}
                    </p>
                  </div>
                  <div>
                    <p className="field-label">Default Wallet</p>
                    <p className="summary-value">
                      {authUser?.walletAddress
                        ? formatAddress(authUser.walletAddress)
                        : formatAddress(monitorInput.trim())}
                    </p>
                  </div>
                </div>

                <div className="alltime-split">
                  <div className="alltime-col">
                    <div className="stat-row">
                      <span>Win Days</span>
                      <span>{winLossDays.wins}</span>
                    </div>
                    <div className="stat-row">
                      <span>Best Day</span>
                      <span>
                        {allTimeStats.bestDay
                          ? `${formatUsd(allTimeStats.bestDay.net)} - ${formatDateLabel(
                              allTimeStats.bestDay.date
                            )}`
                          : '--'}
                      </span>
                    </div>
                    <div className="stat-row">
                      <span>Avg Win Day</span>
                      <span>{formatUsdOptional(lifetimeAvgWin)}</span>
                    </div>
                    <div className="stat-row">
                      <span>Best Win Streak</span>
                      <span>{allTimeStats.winStreak || '--'}</span>
                    </div>
                    <div className="stat-row">
                      <span>Total Volume</span>
                      <span>{formatUsdOptional(allTimeStats.totalVolume)}</span>
                    </div>
                  </div>
                  <div className="alltime-col">
                    <div className="stat-row">
                      <span>Loss Days</span>
                      <span>{winLossDays.losses}</span>
                    </div>
                    <div className="stat-row">
                      <span>Worst Day</span>
                      <span>
                        {allTimeStats.worstDay
                          ? `${formatUsd(allTimeStats.worstDay.net)} - ${formatDateLabel(
                              allTimeStats.worstDay.date
                            )}`
                          : '--'}
                      </span>
                    </div>
                    <div className="stat-row">
                      <span>Avg Loss Day</span>
                      <span>{formatUsdOptional(lifetimeAvgLoss)}</span>
                    </div>
                    <div className="stat-row">
                      <span>Worst Loss Streak</span>
                      <span>{allTimeStats.lossStreak || '--'}</span>
                    </div>
                    <div className="stat-row">
                      <span>Profit Factor</span>
                      <span>{formatRatio(lifetimeProfitFactor)}</span>
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="empty-state">Enter a wallet address to load stats.</div>
            )}
          </section>

          <section className="panel insights" aria-label="Quick insights">
            <div className="panel-header">
              <div>
                <p className="panel-title">Quick Insights</p>
                <p className="panel-subtitle">Trading time, wins/losses, and market focus</p>
              </div>
              <span className="pill soft">Charts</span>
            </div>

            {!hasValidWallet ? (
              <div className="empty-state">Enter a wallet address to view charts.</div>
            ) : (
              <div className="insights-grid">
                <div className="insight-card">
                  <p className="field-label">Trading Time (Hour of Day)</p>
                  {hasTrades ? (
                    <>
                      <svg
                        className="chart chart-bars"
                        viewBox="0 0 240 80"
                        role="img"
                        aria-label="Trades by hour"
                      >
                        {hourlyActivity.buckets.map((count, index) => {
                          const barHeight =
                            (count / hourlyActivity.max) * (80 - 8);
                          const x = index * 10;
                          const y = 80 - barHeight;
                          return (
                            <rect
                              key={`hour-${index}`}
                              x={x}
                              y={y}
                              width={6}
                              height={barHeight}
                              rx={2}
                              className="chart-bar"
                            >
                              <title>{`${index}:00 - ${count} trades`}</title>
                            </rect>
                          );
                        })}
                      </svg>
                      <div className="chart-axis">
                        <span>0</span>
                        <span>6</span>
                        <span>12</span>
                        <span>18</span>
                        <span>23</span>
                      </div>
                    </>
                  ) : (
                    <div className="empty-state">No activity yet.</div>
                  )}
                </div>

                <div className="insight-card">
                  <p className="field-label">Win / Loss Days</p>
                  {hasLifetime ? (
                    <>
                      <div className="stacked-bar" role="img" aria-label="Win loss days">
                        <span
                          className="stacked-segment win"
                          style={{ width: `${winPercent}%` }}
                        />
                        <span
                          className="stacked-segment loss"
                          style={{ width: `${lossPercent}%` }}
                        />
                        <span
                          className="stacked-segment flat"
                          style={{ width: `${flatPercent}%` }}
                        />
                      </div>
                      <div className="stacked-legend">
                        <span className="legend win">Wins {winLossDays.wins}</span>
                        <span className="legend loss">Losses {winLossDays.losses}</span>
                        <span className="legend flat">Flat {winLossDays.flat}</span>
                      </div>
                      <p className="field-note">Based on daily net PnL.</p>
                    </>
                  ) : (
                    <div className="empty-state">Lifetime summary required.</div>
                  )}
                </div>

                <div className="insight-card">
                  <p className="field-label">Daily Net Trend (last 14 days)</p>
                  {hasDailyTrend && dailyNetSparkline.points ? (
                    <>
                      <svg
                        className="chart chart-line"
                        viewBox={`0 0 ${dailyNetSparkline.width} ${dailyNetSparkline.height}`}
                        role="img"
                        aria-label="Daily net trend"
                      >
                        {dailyNetSparkline.zeroY !== null ? (
                          <line
                            x1={0}
                            x2={dailyNetSparkline.width}
                            y1={dailyNetSparkline.zeroY}
                            y2={dailyNetSparkline.zeroY}
                            className="chart-zero"
                          />
                        ) : null}
                        <polyline
                          points={dailyNetSparkline.points}
                          className="chart-line-path"
                        />
                      </svg>
                      <div className="chart-footer">
                        <span>Latest: {formatUsdOptional(dailyNetLatest)}</span>
                        <span>{dailyNetSeries.recent.length} days</span>
                      </div>
                    </>
                  ) : (
                    <div className="empty-state">
                      {hasLifetime
                        ? 'Need at least 2 daily data points.'
                        : 'Lifetime summary required.'}
                    </div>
                  )}
                </div>

                <div className="insight-card">
                  <p className="field-label">Top Markets by USDC</p>
                  {activitySummary.topMarkets.length > 0 ? (
                    <div className="market-bars">
                      {activitySummary.topMarkets.map((entry) => (
                        <div className="market-bar" key={entry.market}>
                          <div className="market-bar-header">
                            <span className="market-bar-label">{entry.market}</span>
                            <span className="market-bar-value">
                              {formatUsd(entry.usdc)}
                            </span>
                          </div>
                          <div className="market-bar-track">
                            <span
                              className="market-bar-fill"
                              style={{
                                width: marketMax
                                  ? `${(entry.usdc / marketMax) * 100}%`
                                  : '0%',
                              }}
                            />
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="empty-state">No market data yet.</div>
                  )}
                </div>
              </div>
            )}
          </section>

            </div>
          </>
        ) : null}

        {activePage === 'trader' ? (
          <>
            <div className="trader-grid">
              <section className="panel trader-card" aria-label="Wallet exports">
                <div className="panel-header">
                  <div>
                    <p className="panel-title">Wallet Exports</p>
                    <p className="panel-subtitle">
                      Download live + historical wallet data as Excel
                    </p>
                  </div>
                  <span className="pill soft">XLSX</span>
                </div>

                <div className="export-grid">
                  <button
                    className="primary-button"
                    onClick={handleFullExport}
                    disabled={exportDisabled}
                  >
                    {exportLoading ? 'Preparing full history...' : 'Download full history'}
                  </button>
                  <button
                    className="ghost-button"
                    onClick={handleSnapshotExport}
                    disabled={!hasValidWallet || exportLoading}
                  >
                    Download live snapshot
                  </button>
                </div>

                <div className="export-meta">
                  <p className="field-label">Included sheets</p>
                  <p className="field-note">Overview, Wallet Activity, PNL, Daily PnL.</p>
                </div>
                <p className="field-note">Full history pulls up to 100,000 trades per wallet.</p>
                <p className="field-note">{snapshotNote}</p>
                {exportNote ? <p className="field-note">{exportNote}</p> : null}
              </section>

              <section className="panel trader-card" aria-label="Range export">
                <div className="panel-header">
                  <div>
                    <p className="panel-title">Range Export</p>
                    <p className="panel-subtitle">Wallet activity range (XLSX)</p>
                  </div>
                  <span className="pill soft">XLSX</span>
                </div>

                <div className="range-export">
                  <div className="history-grid">
                    <div className="input-stack">
                      <label className="field-label" htmlFor="range-start">
                        Start date
                      </label>
                      <input
                        id="range-start"
                        className="text-input"
                        type="date"
                        value={rangeExportStart}
                        onChange={(event) => {
                          setRangeExportStart(event.target.value);
                          if (rangeExportEarliestError) {
                            setRangeExportEarliestError(null);
                          }
                        }}
                      />
                      <button
                        className="text-button"
                        type="button"
                        onClick={handleRangeExportUseEarliest}
                        disabled={!hasValidWallet || rangeExportEarliestLoading}
                      >
                        {rangeExportEarliestLoading
                          ? 'Finding earliest...'
                          : 'Use earliest trade'}
                      </button>
                      {rangeExportEarliestError ? (
                        <p className="field-note error">{rangeExportEarliestError}</p>
                      ) : null}
                    </div>
                    <div className="input-stack">
                      <label className="field-label" htmlFor="range-end">
                        End date
                      </label>
                      <input
                        id="range-end"
                        className="text-input"
                        type="date"
                        value={rangeExportEnd}
                        onChange={(event) => setRangeExportEnd(event.target.value)}
                      />
                    </div>
                    <div className="input-stack">
                      <label className="field-label" htmlFor="range-max">
                        Max rows
                      </label>
                      <input
                        id="range-max"
                        className="text-input"
                        placeholder="5000"
                        value={rangeExportLimit}
                        onChange={(event) => setRangeExportLimit(event.target.value)}
                      />
                    </div>
                  </div>
                  <div className="history-actions">
                    <button
                      className="primary-button"
                      type="button"
                      onClick={handleRangeExport}
                      disabled={!hasValidWallet || rangeExportLoading}
                    >
                      {rangeExportLoading ? 'Preparing range...' : 'Download range XLSX'}
                    </button>
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={handleRangeExportEverything}
                      disabled={!hasValidWallet || rangeExportLoading || rangeExportEarliestLoading}
                    >
                      Export everything
                    </button>
                    {rangeExportLoading ? (
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={handleRangeExportCancel}
                      >
                        Cancel
                      </button>
                    ) : null}
                  </div>
                  {rangeExportLoading ? (
                    <p className="field-note">
                      Fetched {rangeExportProgress} rows in {rangeExportPages} batches.
                      {rangeExportCancelRequested ? ' Stopping after current request...' : ''}
                    </p>
                  ) : null}
                  {!rangeExportLoading && rangeExportResumeCursor !== null ? (
                    <div className="range-resume">
                      <p className="field-note">
                        Resume available from {rangeExportResumeCursor} (
                        {formatTimestamp(rangeExportResumeCursor)}).
                      </p>
                      <button
                        className="ghost-button"
                        type="button"
                        onClick={handleRangeExportResume}
                      >
                        Resume from last cursor
                      </button>
                    </div>
                  ) : null}
                  <p className="field-note">
                    Leave start/end blank to export the latest Max rows.
                  </p>
                  <p className="field-note">
                    Large exports split into multiple XLSX files to avoid browser limits.
                  </p>
                  {rangeExportError ? (
                    <p className="field-note error">{rangeExportError}</p>
                  ) : null}
                  {rangeExportWarning ? (
                    <p className="field-note">{rangeExportWarning}</p>
                  ) : null}
                </div>
              </section>

              

              <section
                className="panel trader-card trader-card-wide"
                aria-label="Trader positions"
              >
                <div className="panel-header">
                  <div>
                    <p className="panel-title">Positions</p>
                    <p className="panel-subtitle">
                      Active and closed positions from Polymarket data API
                    </p>
                  </div>
                  <div className="panel-actions">
                    <div className="admin-tabs">
                      <button
                        className={`chip chip-button ${
                          positionsTab === 'active' ? 'is-active' : ''
                        }`}
                        type="button"
                        onClick={() => setPositionsTab('active')}
                      >
                        Active ({activePositionsTotal})
                      </button>
                      <button
                        className={`chip chip-button ${
                          positionsTab === 'closed' ? 'is-active' : ''
                        }`}
                        type="button"
                        onClick={() => setPositionsTab('closed')}
                      >
                        Closed ({closedPositionsTotal})
                      </button>
                    </div>
                  </div>
                </div>

                <div className="history-grid">
                  <div className="input-stack">
                    <label className="field-label" htmlFor="positions-wallet">
                      Wallet address
                    </label>
                    <input
                      id="positions-wallet"
                      className="text-input"
                      placeholder={trimmedWallet || '0x...'}
                      value={positionsWalletInput}
                      onChange={(event) => setPositionsWalletInput(event.target.value)}
                    />
                    {!positionsWalletInput.trim() ? (
                      <p className="field-note">Leave blank to use monitored wallet.</p>
                    ) : null}
                    <button
                      className="text-button"
                      type="button"
                      onClick={() => setPositionsWalletInput(trimmedWallet)}
                      disabled={!hasValidWallet}
                    >
                      Use monitored wallet
                    </button>
                  </div>
                  <div className="input-stack">
                    <label className="field-label" htmlFor="positions-limit">
                      Rows per page
                    </label>
                    <select
                      id="positions-limit"
                      className="text-input"
                      value={positionsPageSize}
                      onChange={(event) => setPositionsPageSize(event.target.value)}
                    >
                      <option value="25">25</option>
                      <option value="50">50</option>
                      <option value="100">100</option>
                      <option value="200">200</option>
                    </select>
                  </div>
                </div>

                <div className="history-actions">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={handlePositionsPrev}
                    disabled={positionsOffset === 0 || positionsLoading}
                  >
                    Prev
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={handlePositionsNext}
                    disabled={!positionsHasNext || positionsLoading}
                  >
                    Next
                  </button>
                  {positionsCount !== null && positionsCount !== undefined ? (
                    <span className="pill soft">
                      {positionsPageStart}-{positionsPageEnd} of {positionsCount}
                    </span>
                  ) : positionsRows.length > 0 ? (
                    <span className="pill soft">
                      {positionsPageStart}-{positionsPageEnd}
                    </span>
                  ) : null}
                </div>

                {positionsError ? (
                  <p className="field-note error">{positionsError}</p>
                ) : null}

                {positionsLoading && positionsRows.length === 0 ? (
                  <div className="empty-state">Loading positions...</div>
                ) : positionsRows.length === 0 ? (
                  <div className="empty-state">No positions found.</div>
                ) : (
                  <div className="history-table positions-table">
                    <div className="history-table-header">
                      <span>market</span>
                      <span>outcome</span>
                      {positionsTab === 'active' ? (
                        <>
                          <span>size</span>
                          <span>avg price</span>
                          <span>current value</span>
                          <span>pnl</span>
                        </>
                      ) : (
                        <>
                          <span>total bought</span>
                          <span>avg price</span>
                          <span>realized pnl</span>
                          <span>closed</span>
                        </>
                      )}
                    </div>
                    {positionsRows.map((position, index) => {
                      const marketLabel =
                        position.title || position.slug || position.eventSlug || 'Market';
                      const closedLabel = position.endDate
                        ? formatDateLabel(position.endDate)
                        : position.timestamp
                          ? formatTimestamp(position.timestamp)
                          : '--';
                      const pnlValue = position.cashPnl ?? position.realizedPnl ?? 0;
                      return (
                        <div
                          className="history-table-row"
                          key={`${position.asset || marketLabel}-${position.outcome || ''}-${index}`}
                        >
                          <span title={marketLabel}>{marketLabel}</span>
                          <span>{position.outcome || '--'}</span>
                          {positionsTab === 'active' ? (
                            <>
                              <span>{formatShares(position.size)}</span>
                              <span>{formatTokenPrice(position.avgPrice)}</span>
                              <span>{formatUsd(position.currentValue)}</span>
                              <span className={getPnlClass(pnlValue)}>
                                {formatPositionPnl(position)}
                              </span>
                            </>
                          ) : (
                            <>
                              <span>{formatUsd(position.totalBought)}</span>
                              <span>{formatTokenPrice(position.avgPrice)}</span>
                              <span className={getPnlClass(pnlValue)}>
                                {formatPositionPnl(position)}
                              </span>
                              <span>{closedLabel}</span>
                            </>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </section>

              

              <section className="panel trader-card trader-card-wide" aria-label="Trades preview">
                <div className="panel-header">
                  <div>
                    <p className="panel-title">Trades Preview</p>
                    <p className="panel-subtitle">Loaded from the last range export</p>
                  </div>
                  {rangeExportPreviewTotal !== null ? (
                    <span className="pill soft">{rangeExportPreviewTotal} trades</span>
                  ) : null}
                </div>

                {rangeExportPreview.length === 0 ? (
                  <div className="empty-state">Run a range export to load trades.</div>
                ) : (
                  <>
                    <div className="history-table">
                      <div className="history-table-header">
                        <span>time</span>
                        <span>market</span>
                        <span>outcome</span>
                        <span>side</span>
                        <span>price</span>
                        <span>usdc</span>
                      </div>
                      {rangeExportPreview.map((trade) => (
                        <div className="history-table-row" key={getTradeKey(trade)}>
                          <span>{formatTimestamp(trade.timestamp)}</span>
                          <span title={trade.title || trade.slug || trade.eventSlug || ''}>
                            {trade.title || trade.slug || trade.eventSlug || 'Market'}
                          </span>
                          <span>{trade.outcome || '--'}</span>
                          <span>{normalizeSide(trade.side).toUpperCase()}</span>
                          <span>{formatTokenPrice(trade.price)}</span>
                          <span>{formatUsd(trade.usdcSize)}</span>
                        </div>
                      ))}
                    </div>
                    <p className="field-note">
                      Showing first {rangeExportPreview.length} of{' '}
                      {rangeExportPreviewTotal ?? rangeExportPreview.length} trades.
                    </p>
                  </>
                )}
              </section>
            </div>
          </>
        ) : null}

        {activePage === 'market' ? (
          <div className="market-tools tools">
            <section className="panel live-stream" aria-label="Live market stream">
              <div className="panel-header">
                <div>
                  <p className="panel-title">Live Market Stream</p>
                  <p className="panel-subtitle">
                    WebSocket feed for live prices and order books
                  </p>
                </div>
                <div className="panel-actions">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={handleLiveStreamClear}
                    disabled={liveStreamMessages.length === 0}
                  >
                    Clear
                  </button>
                  <span className={`status-pill ${liveStreamStatusClass}`}>
                    {liveStreamStatusLabel}
                  </span>
                </div>
              </div>

              <div className="history-controls">
                <div className="history-grid">
                  <div className="input-stack">
                    <label className="field-label" htmlFor="live-stream-slug">
                      Market slug (optional)
                    </label>
                    <input
                      id="live-stream-slug"
                      className="text-input"
                      placeholder="btc-updown-15m-1769320800"
                      value={liveStreamSlug}
                      onChange={(event) => setLiveStreamSlug(event.target.value)}
                      disabled={
                        liveStreamConnected || liveStreamConnecting || liveStreamResolving
                      }
                    />
                  </div>
                  <div className="input-stack">
                    <label className="field-label" htmlFor="live-stream-token-ids">
                      Token IDs
                    </label>
                    <input
                      id="live-stream-token-ids"
                      className="text-input"
                      placeholder="Comma or space separated"
                      value={liveStreamTokenIds}
                      onChange={(event) => setLiveStreamTokenIds(event.target.value)}
                      disabled={
                        liveStreamConnected || liveStreamConnecting || liveStreamResolving
                      }
                    />
                  </div>
                  <div className="input-stack">
                    <label className="field-label">Channels</label>
                    <div className="toggle">
                  <button
                    className={`toggle-button ${
                      liveStreamChannels.market ? 'is-active' : ''
                    }`}
                    type="button"
                    onClick={() =>
                      setLiveStreamChannels((prev) => ({
                        ...prev,
                        market: !prev.market,
                      }))
                    }
                    disabled={liveStreamConnected || liveStreamConnecting || liveStreamResolving}
                  >
                    Market
                  </button>
                  <button
                    className={`toggle-button ${
                      liveStreamChannels.book ? 'is-active' : ''
                    }`}
                    type="button"
                    onClick={() =>
                      setLiveStreamChannels((prev) => ({
                        ...prev,
                        book: !prev.book,
                      }))
                    }
                    disabled={liveStreamConnected || liveStreamConnecting || liveStreamResolving}
                  >
                    Order book
                  </button>
                    </div>
                  </div>
                </div>

                <div className="history-actions">
                  <button
                    className="primary-button"
                    type="button"
                    onClick={handleLiveStreamConnect}
                    disabled={
                      liveStreamConnected || liveStreamConnecting || liveStreamResolving
                    }
                  >
                    {liveStreamResolving
                      ? 'Resolving...'
                      : liveStreamConnecting
                        ? 'Connecting...'
                        : 'Connect'}
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={handleLiveStreamDisconnect}
                    disabled={
                      !liveStreamConnected && !liveStreamConnecting && !liveStreamPolling
                    }
                  >
                    Disconnect
                  </button>
                  <button
                    className="ghost-button danger-button"
                    type="button"
                    onClick={handleLiveStreamStop}
                    disabled={!liveStreamAutoContinue}
                  >
                    Stop Auto
                  </button>
                </div>

                <p className="field-note">
                  If token IDs are blank, the slug is used to resolve them.
                </p>
                <p className="field-note">
                  Slugs with an interval (ex: -15m) auto-advance and export XLSX per
                  market.
                </p>
                {liveStreamError ? (
                  <p className="field-note error">{liveStreamError}</p>
                ) : null}
                {liveStreamWarning ? (
                  <p className="field-note">{liveStreamWarning}</p>
                ) : null}
                {liveStreamPolling ? (
                  <p className="field-note">
                    REST polling fallback active every {LIVE_STREAM_POLL_INTERVAL_MS / 1000}
                    s.
                  </p>
                ) : null}
              </div>

              {liveStreamStatsList.length > 0 ? (
                <div className="breakdown-list live-stream-metrics">
                  {liveStreamStatsList.map((stat) => (
                    <div className="breakdown-row" key={stat.tokenId}>
                      <span className="breakdown-name">{stat.tokenId}</span>
                      <span className="breakdown-value">
                        Bid {formatTokenPrice(stat.bestBid ?? null)} | Ask{' '}
                        {formatTokenPrice(stat.bestAsk ?? null)} | Mid{' '}
                        {formatTokenPrice(stat.mid ?? null)} | Spread{' '}
                        {stat.spread !== undefined && stat.spread !== null
                          ? stat.spread.toFixed(4)
                          : '--'}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-state">No live order book updates yet.</div>
              )}

              {liveStreamVizTokens.length > 0 ? (
                <div className="live-stream-viz">
                  {liveStreamVizTokens.map((tokenId) => {
                    const history = liveStreamPriceHistory[tokenId];
                    const points = history?.points ?? [];
                    const bidPoints = history?.bidPoints ?? [];
                    const askPoints = history?.askPoints ?? [];
                    const lastPrice =
                      points.length > 0 ? points[points.length - 1] : null;
                    const bidPath = buildSparklinePath(bidPoints, 320, 110);
                    const askPath = buildSparklinePath(askPoints, 320, 110);
                    const stats = liveStreamBookStats[tokenId];
                    const bid = stats?.bestBid ?? null;
                    const ask = stats?.bestAsk ?? null;
                    const bidPct =
                      bid !== null && bid !== undefined
                        ? Math.min(100, Math.max(0, bid * 100))
                        : 0;
                    const askPct =
                      ask !== null && ask !== undefined
                        ? Math.min(100, Math.max(0, ask * 100))
                        : 0;
                    return (
                      <div className="live-stream-viz-card" key={tokenId}>
                        <div className="live-stream-viz-header">
                          <div>
                            <p className="live-stream-viz-title">
                              {formatTokenLabel(tokenId)}
                            </p>
                            <p className="live-stream-viz-subtitle">{tokenId}</p>
                          </div>
                          <div className="live-stream-viz-price">
                            {lastPrice !== null ? formatTokenPrice(lastPrice) : '--'}
                          </div>
                        </div>
                        <div className="live-stream-sparkline">
                          {bidPath || askPath ? (
                            <svg
                              viewBox="0 0 320 110"
                              role="img"
                              aria-label="Price sparkline"
                            >
                              {bidPath ? (
                                <path className="bid" d={bidPath} />
                              ) : null}
                              {askPath ? (
                                <path className="ask" d={askPath} />
                              ) : null}
                            </svg>
                          ) : (
                            <span className="live-stream-empty">
                              No price updates yet.
                            </span>
                          )}
                        </div>
                        <div className="live-stream-bars">
                          <div className="live-stream-bar-row">
                            <span className="live-stream-bar-label">Bid</span>
                            <div className="live-stream-bar">
                              <div
                                className="live-stream-bar-fill bid"
                                style={{ width: `${bidPct}%` }}
                              />
                            </div>
                            <span className="live-stream-bar-value">
                              {formatTokenPrice(bid)}
                            </span>
                          </div>
                          <div className="live-stream-bar-row">
                            <span className="live-stream-bar-label">Ask</span>
                            <div className="live-stream-bar">
                              <div
                                className="live-stream-bar-fill ask"
                                style={{ width: `${askPct}%` }}
                              />
                            </div>
                            <span className="live-stream-bar-value">
                              {formatTokenPrice(ask)}
                            </span>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : null}

              <div className="live-stream-feed">
                {liveStreamMessages.length === 0 ? (
                  <div className="empty-state">Awaiting live stream messages.</div>
                ) : (
                  liveStreamMessages.map((message) => (
                    <div className="live-stream-line" key={message.id}>
                      <span className="live-stream-time">
                        {new Date(message.timestamp).toLocaleTimeString()}
                      </span>
                      <span className="live-stream-summary">{message.summary}</span>
                      <code className="live-stream-raw">{message.raw}</code>
                    </div>
                  ))
                )}
              </div>

              {liveStreamLatestPayload ? (
                <div className="live-stream-payload">
                  <p className="field-label">Latest payload</p>
                  <pre className="live-stream-raw-block">
                    {liveStreamLatestPayload}
                  </pre>
                </div>
              ) : null}
            </section>
            <section className="panel market-data" aria-label="Market data blueprint">
              <div className="panel-header">
                <div>
                  <p className="panel-title">Market Data Blueprint</p>
                  <p className="panel-subtitle">
                    CLOB + Gamma API feed map for live analysis
                  </p>
                </div>
                <span className="pill soft">Live</span>
              </div>

              <p className="market-data-lede">
                Collect all live market data, including order books and real-time prices,
                using Polymarket's Central Limit Order Book (CLOB) API and Gamma API.
                This programmatic access is ideal for high-fidelity market analysis and
                automated trading.
              </p>

              <div className="market-data-grid">
                <div className="market-data-column">
                  <div className="market-data-card">
                    <div className="market-data-heading">
                      <span className="chip">1</span>
                      <p className="market-data-title">Collecting live market data</p>
                    </div>
                    <ul className="market-data-list">
                      <li>
                        <strong>Live market discovery:</strong> use{' '}
                        <code>GET /events</code> or <code>GET /markets</code> with{' '}
                        <code>active=true</code> and <code>closed=false</code> via Gamma
                        API.
                      </li>
                      <li>
                        <strong>Real-time price and book streams:</strong> subscribe to
                        the CLOB WebSocket market channel at{' '}
                        <code>wss://ws-subscriptions-clob.polymarket.com/ws/</code>.
                      </li>
                      <li>
                        <strong>Snapshots plus streams:</strong> combine REST snapshots
                        with WebSocket deltas for the live experience.
                      </li>
                    </ul>
                  </div>

                  <div className="market-data-card">
                    <div className="market-data-heading">
                      <span className="chip">2</span>
                      <p className="market-data-title">Collecting the order book</p>
                    </div>
                    <ul className="market-data-list">
                      <li>
                        <strong>Snapshot:</strong>{' '}
                        <code>GET /book?token_id=TOKEN_ID</code> for top bids and asks.
                      </li>
                      <li>
                        <strong>Market metrics:</strong> best bid and ask, midpoint price,
                        spread (ex: 0.3c) to measure efficiency and liquidity.
                      </li>
                    </ul>
                  </div>

                  <div className="market-data-card">
                    <div className="market-data-heading">
                      <span className="chip">3</span>
                      <p className="market-data-title">Price to beat and current price</p>
                    </div>
                    <ul className="market-data-list">
                      <li>
                        <strong>Price to beat:</strong> best ask when buying, best bid
                        when selling. Any better limit order becomes the new price to
                        beat.
                      </li>
                      <li>
                        <strong>Current price:</strong>{' '}
                        <code>GET /price?token_id=TOKEN_ID&amp;side=BUY|SELL</code> for
                        the latest traded price by side.
                      </li>
                    </ul>
                  </div>
                </div>

                <div className="market-data-column">
                  <div className="market-data-card summary">
                    <p className="market-data-title">Market analysis tools</p>
                    <div className="market-data-table">
                      <div className="market-data-row market-data-header">
                        <span>Feature</span>
                        <span>Endpoint / Tool</span>
                        <span>Benefit for analysis</span>
                      </div>
                      <div className="market-data-row">
                        <span>Market discovery</span>
                        <span>
                          <code>Gamma /events</code>
                        </span>
                        <span>Finds new high-volume opportunities.</span>
                      </div>
                      <div className="market-data-row">
                        <span>Real-time odds</span>
                        <span>
                          <code>CLOB WebSocket (market)</code>
                        </span>
                        <span>Tracks probability shifts second-by-second.</span>
                      </div>
                      <div className="market-data-row">
                        <span>Liquidity depth</span>
                        <span>
                          <code>CLOB /book</code>
                        </span>
                        <span>
                          Measures how much USDC can be traded without slipping.
                        </span>
                      </div>
                      <div className="market-data-row">
                        <span>Historical trends</span>
                        <span>
                          <code>CLOB /prices-history</code>
                        </span>
                        <span>Charts price volatility and momentum over time.</span>
                      </div>
                    </div>
                  </div>

                  <div className="market-data-callout">
                    <p className="market-data-callout-title">Programmatic access</p>
                    <p className="market-data-callout-body">
                      Blend REST snapshots with WebSocket updates to drive automated
                      trading and high-fidelity market analysis.
                    </p>
                  </div>
                </div>
              </div>
            </section>
            
          </div>
        ) : null}
      </main>

      {isAdmin && activePage === 'trader' ? (
        <section className="panel admin-panel" aria-label="Admin dashboard">
          <div className="panel-header">
            <div>
              <p className="panel-title">Admin Dashboard</p>
              <p className="panel-subtitle">Manage users and review activity logs</p>
            </div>
            <div className="panel-actions">
              <div className="admin-tabs">
                <button
                  className={`chip chip-button ${adminTab === 'users' ? 'is-active' : ''}`}
                  onClick={() => setAdminTab('users')}
                  type="button"
                >
                  Users
                </button>
                <button
                  className={`chip chip-button ${adminTab === 'activity' ? 'is-active' : ''}`}
                  onClick={() => setAdminTab('activity')}
                  type="button"
                >
                  Activity
                </button>
              </div>
              <button className="ghost-button" onClick={handleAdminRefresh} type="button">
                Refresh
              </button>
            </div>
          </div>

          {adminLoading ? (
            <div className="empty-state">Loading admin data...</div>
          ) : adminError ? (
            <div className="empty-state">{adminError}</div>
          ) : adminTab === 'users' ? (
            <div className="admin-list">
              {adminUsers.length === 0 ? (
                <div className="empty-state">No users available.</div>
              ) : (
                adminUsers.map((user) => (
                  <div className="admin-row" key={user.id}>
                    <div>
                      <p className="admin-title">{user.email}</p>
                      <p className="admin-meta">
                        Role: {user.role} · Wallet:{' '}
                        {user.walletAddress ? formatAddress(user.walletAddress) : '--'} · Created:{' '}
                        {formatIso(user.createdAt)}
                      </p>
                      <p className="admin-meta">
                        Last login: {formatIso(user.lastLoginAt)}
                      </p>
                    </div>
                    <div className="admin-actions">
                      <span className={`pill ${user.role === 'admin' ? 'accent' : 'soft'}`}>
                        {user.role}
                      </span>
                      <button
                        className="ghost-button"
                        onClick={() => handleRoleToggle(user)}
                        type="button"
                      >
                        {user.role === 'admin' ? 'Make user' : 'Make admin'}
                      </button>
                    </div>
                  </div>
                ))
              )}
            </div>
          ) : (
            <div className="admin-list">
              {adminActivity.length === 0 ? (
                <div className="empty-state">No activity yet.</div>
              ) : (
                adminActivity.map((entry) => (
                  <div className="admin-row" key={entry.id}>
                    <div>
                      <p className="admin-title">{entry.action}</p>
                      <p className="admin-meta">
                        {entry.user ? entry.user.email : 'Unknown user'} ·{' '}
                        {formatIso(entry.createdAt)}
                      </p>
                      <p className="admin-meta">Metadata: {formatMetadata(entry.metadata)}</p>
                    </div>
                    <div className="admin-actions">
                      <span className="pill soft">{entry.user?.role ?? 'user'}</span>
                    </div>
                  </div>
                ))
              )}
            </div>
          )}
        </section>
      ) : null}
    </div>
  );
}
