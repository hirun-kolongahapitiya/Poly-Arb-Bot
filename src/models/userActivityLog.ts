import { FileCollection } from '../storage/fileStore';

export type UserActivityLogRecord = {
    _id: string;
    userId: string;
    action: string;
    metadata: Record<string, unknown>;
    createdAt?: string;
};

const UserActivityLog = new FileCollection<UserActivityLogRecord>('user_activity_logs.json', {
    timestamps: 'created',
});

export default UserActivityLog;
