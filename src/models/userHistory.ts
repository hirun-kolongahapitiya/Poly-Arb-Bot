import { UserActivityInterface, UserPositionInterface } from '../interfaces/User';
import { FileCollection, sanitizeFilePart } from '../storage/fileStore';

const activityModels = new Map<string, FileCollection<UserActivityInterface>>();
const positionModels = new Map<string, FileCollection<UserPositionInterface>>();

const buildFileKey = (walletAddress: string): string =>
    sanitizeFilePart(walletAddress || 'unknown');

const getUserPositionModel = (walletAddress: string) => {
    const key = buildFileKey(walletAddress);
    const existing = positionModels.get(key);
    if (existing) {
        return existing;
    }
    const model = new FileCollection<UserPositionInterface>(`user_positions_${key}.json`);
    positionModels.set(key, model);
    return model;
};

const getUserActivityModel = (walletAddress: string) => {
    const key = buildFileKey(walletAddress);
    const existing = activityModels.get(key);
    if (existing) {
        return existing;
    }
    const model = new FileCollection<UserActivityInterface>(`user_activities_${key}.json`);
    activityModels.set(key, model);
    return model;
};

export { getUserActivityModel, getUserPositionModel };
