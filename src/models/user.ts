import { FileCollection } from '../storage/fileStore';

export type UserRecord = {
    _id: string;
    email: string;
    passwordHash: string;
    role: 'user' | 'admin';
    walletAddress: string;
    lastLoginAt: string | null;
    createdAt?: string;
    updatedAt?: string;
};

const UserModel = new FileCollection<UserRecord>('users.json', {
    timestamps: 'created-updated',
});

export default UserModel;
