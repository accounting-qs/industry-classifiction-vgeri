import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { fetchDigest } from '../services/scraperService';

// Run test script: npx tsx scripts/test-scrapers.ts

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables from .env.local
dotenv.config({ path: path.join(__dirname, '../.env.local') });

const testUrl = 'http://www.petersfinancial.us';

async function runTests() {
    try {
        const { digest, proxyName } = await fetchDigest(testUrl, msg => console.log(msg));
        console.log('✅ Success! Proxy:', proxyName);
        console.log('Digest Length:', digest.length);
        console.log('\n--- DIGEST START ---\n' + digest + '\n--- DIGEST END ---\n');
    } catch (e: any) {
        console.error('❌ Failed:', e.message);
    }
}

runTests().catch(console.error);
