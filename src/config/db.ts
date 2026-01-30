import chalk from 'chalk';
import { ensureStorageDir } from '../storage/fileStore';

let initialized = false;

const connectDB = async () => {
    if (initialized) {
        return;
    }
    try {
        const dir = await ensureStorageDir();
        console.log(chalk.green('âœ“'), `File storage ready: ${dir}`);
        initialized = true;
    } catch (error) {
        console.log(chalk.red('âœ—'), 'File storage initialization failed:', error);
        process.exit(1);
    }
};

export const closeDB = async (): Promise<void> => {
    try {
        initialized = false;
        console.log(chalk.green('âœ“'), 'File storage closed');
    } catch (error) {
        console.log(chalk.red('âœ—'), 'Error closing file storage:', error);
    }
};

export default connectDB;
