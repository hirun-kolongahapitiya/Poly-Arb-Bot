import { randomBytes } from 'crypto';
import { promises as fs } from 'fs';
import * as path from 'path';

export type SortDirection = 1 | -1;
export type SortSpec = Record<string, SortDirection>;

export type Filter =
    | Record<string, unknown>
    | {
          $and?: Filter[];
      };

export type Update =
    | Record<string, unknown>
    | {
          $set?: Record<string, unknown>;
      };

export type FindOptions = {
    sort?: SortSpec;
    limit?: number;
    skip?: number;
};

type TimestampMode = 'none' | 'created' | 'created-updated';

const DEFAULT_STORAGE_DIR = path.resolve(process.cwd(), 'data');

export const getStorageDir = (): string => {
    const custom = process.env.STORAGE_DIR?.trim();
    if (custom) {
        return path.isAbsolute(custom) ? custom : path.resolve(process.cwd(), custom);
    }
    return DEFAULT_STORAGE_DIR;
};

export const ensureStorageDir = async (): Promise<string> => {
    const dir = getStorageDir();
    await fs.mkdir(dir, { recursive: true });
    return dir;
};

export const sanitizeFilePart = (value: string): string =>
    value.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');

const createId = (): string => randomBytes(12).toString('hex');

const readJsonFile = async <T>(filePath: string): Promise<T[]> => {
    try {
        const raw = await fs.readFile(filePath, 'utf-8');
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? (parsed as T[]) : [];
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
            return [];
        }
        throw error;
    }
};

const writeJsonFile = async <T>(filePath: string, data: T[]): Promise<void> => {
    await ensureStorageDir();
    const content = JSON.stringify(data, null, 2);
    await fs.writeFile(filePath, content, 'utf-8');
};

const normalizeSortValue = (value: unknown): number | string | null => {
    if (value === null || value === undefined) {
        return null;
    }
    if (value instanceof Date) {
        return value.getTime();
    }
    if (typeof value === 'number') {
        return value;
    }
    if (typeof value === 'string') {
        const parsed = Date.parse(value);
        if (!Number.isNaN(parsed)) {
            return parsed;
        }
        return value.toLowerCase();
    }
    return String(value);
};

const compareValues = (a: unknown, b: unknown): number => {
    const left = normalizeSortValue(a);
    const right = normalizeSortValue(b);
    if (left === right) {
        return 0;
    }
    if (left === null) {
        return -1;
    }
    if (right === null) {
        return 1;
    }
    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
};

const matchesOperator = (value: unknown, condition: Record<string, unknown>): boolean => {
    if ('$exists' in condition) {
        const shouldExist = Boolean(condition.$exists);
        const exists = value !== undefined;
        if (exists !== shouldExist) {
            return false;
        }
    }
    if ('$gt' in condition) {
        if (typeof value !== 'number' || typeof condition.$gt !== 'number') {
            return false;
        }
        if (!(value > condition.$gt)) {
            return false;
        }
    }
    if ('$in' in condition) {
        const list = Array.isArray(condition.$in) ? condition.$in : [];
        if (!list.includes(value)) {
            return false;
        }
    }
    return true;
};

const matchesFilter = (item: Record<string, unknown>, filter?: Filter): boolean => {
    if (!filter || Object.keys(filter).length === 0) {
        return true;
    }
    if ('$and' in filter && Array.isArray(filter.$and)) {
        return filter.$and.every((entry) => matchesFilter(item, entry));
    }
    return Object.entries(filter).every(([key, condition]) => {
        if (key === '$and') {
            return true;
        }
        const value = item[key];
        if (condition && typeof condition === 'object' && !Array.isArray(condition)) {
            const opKeys = Object.keys(condition);
            if (opKeys.some((op) => op.startsWith('$'))) {
                return matchesOperator(value, condition as Record<string, unknown>);
            }
        }
        return value === condition;
    });
};

const applyUpdate = (item: Record<string, unknown>, update: Update): Record<string, unknown> => {
    const next: Record<string, unknown> = { ...item };
    if (update && typeof update === 'object' && '$set' in update) {
        const setValues = (update as { $set?: Record<string, unknown> }).$set || {};
        for (const [key, value] of Object.entries(setValues)) {
            next[key] = value;
        }
        return next;
    }
    for (const [key, value] of Object.entries(update)) {
        next[key] = value;
    }
    return next;
};

const applyTimestamps = (
    item: Record<string, unknown>,
    mode: TimestampMode,
    isCreate: boolean
): Record<string, unknown> => {
    if (mode === 'none') {
        return item;
    }
    const now = new Date().toISOString();
    if (isCreate) {
        if (mode === 'created' || mode === 'created-updated') {
            item.createdAt = now;
        }
        if (mode === 'created-updated') {
            item.updatedAt = now;
        }
    } else if (mode === 'created-updated') {
        item.updatedAt = now;
    }
    return item;
};

export class FileCollection<T extends { _id: string }> {
    private filePath: string;
    private timestamps: TimestampMode;

    constructor(fileName: string, options?: { timestamps?: TimestampMode }) {
        this.filePath = path.join(getStorageDir(), fileName);
        this.timestamps = options?.timestamps ?? 'none';
    }

    private async readAll(): Promise<T[]> {
        return readJsonFile<T>(this.filePath);
    }

    private async writeAll(items: T[]): Promise<void> {
        await writeJsonFile(this.filePath, items);
    }

    async find(filter?: Filter, options?: FindOptions): Promise<T[]> {
        const items = (await this.readAll()).filter((item) => matchesFilter(item, filter));
        const sorted = options?.sort
            ? [...items].sort((a, b) => {
                  for (const [field, direction] of Object.entries(options.sort || {})) {
                      const left = (a as Record<string, unknown>)[field];
                      const right = (b as Record<string, unknown>)[field];
                      const comparison = compareValues(left, right);
                      if (comparison !== 0) {
                          return comparison * direction;
                      }
                  }
                  return 0;
              })
            : items;
        const skipped = typeof options?.skip === 'number' ? sorted.slice(options.skip) : sorted;
        if (typeof options?.limit === 'number') {
            return skipped.slice(0, options.limit);
        }
        return skipped;
    }

    async findOne(filter?: Filter): Promise<T | null> {
        const items = await this.find(filter, { limit: 1 });
        return items[0] ?? null;
    }

    async findById(id?: string | null): Promise<T | null> {
        if (!id) {
            return null;
        }
        return this.findOne({ _id: id });
    }

    async countDocuments(filter?: Filter): Promise<number> {
        if (!filter || Object.keys(filter).length === 0) {
            const items = await this.readAll();
            return items.length;
        }
        const items = await this.find(filter);
        return items.length;
    }

    async create(data: Omit<T, '_id'> & Partial<{ _id: string }>): Promise<T> {
        const items = await this.readAll();
        const record = applyTimestamps(
            {
                ...data,
                _id: data._id ?? createId(),
            } as Record<string, unknown>,
            this.timestamps,
            true
        ) as T;
        items.push(record);
        await this.writeAll(items);
        return record;
    }

    async updateOne(
        filter: Filter,
        update: Update
    ): Promise<{ matchedCount: number; modifiedCount: number }> {
        const items = await this.readAll();
        let matched = 0;
        let modified = 0;
        const nextItems = items.map((item) => {
            if (matched === 0 && matchesFilter(item, filter)) {
                matched = 1;
                modified = 1;
                const updated = applyUpdate(item as Record<string, unknown>, update);
                return applyTimestamps(updated, this.timestamps, false) as T;
            }
            return item;
        });
        if (modified > 0) {
            await this.writeAll(nextItems);
        }
        return { matchedCount: matched, modifiedCount: modified };
    }

    async updateMany(
        filter: Filter,
        update: Update
    ): Promise<{ matchedCount: number; modifiedCount: number }> {
        const items = await this.readAll();
        let matched = 0;
        let modified = 0;
        const nextItems = items.map((item) => {
            if (matchesFilter(item, filter)) {
                matched += 1;
                modified += 1;
                const updated = applyUpdate(item as Record<string, unknown>, update);
                return applyTimestamps(updated, this.timestamps, false) as T;
            }
            return item;
        });
        if (modified > 0) {
            await this.writeAll(nextItems);
        }
        return { matchedCount: matched, modifiedCount: modified };
    }

    async findOneAndUpdate(
        filter: Filter,
        update: Update,
        options?: { upsert?: boolean; new?: boolean }
    ): Promise<T | null> {
        const items = await this.readAll();
        const index = items.findIndex((item) => matchesFilter(item, filter));
        if (index >= 0) {
            const original = items[index];
            const updated = applyTimestamps(
                applyUpdate(original as Record<string, unknown>, update),
                this.timestamps,
                false
            ) as T;
            items[index] = updated;
            await this.writeAll(items);
            return options?.new ? updated : original;
        }
        if (options?.upsert) {
            const base =
                filter && typeof filter === 'object' && !Array.isArray(filter)
                    ? Object.entries(filter).reduce<Record<string, unknown>>((acc, [key, value]) => {
                          if (!key.startsWith('$')) {
                              acc[key] = value;
                          }
                          return acc;
                      }, {})
                    : {};
            const record = applyTimestamps(
                {
                    ...base,
                    ...applyUpdate({}, update),
                    _id: createId(),
                },
                this.timestamps,
                true
            ) as T;
            items.push(record);
            await this.writeAll(items);
            return record;
        }
        return null;
    }

    async findByIdAndUpdate(
        id: string,
        update: Update,
        options?: { new?: boolean }
    ): Promise<T | null> {
        return this.findOneAndUpdate({ _id: id }, update, options);
    }
}
