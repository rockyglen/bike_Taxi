const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const path = require('path');
const fs = require('fs');

const parentEnvPath = path.resolve(__dirname, '../.env');
if (fs.existsSync(parentEnvPath)) {
    const lines = fs.readFileSync(parentEnvPath, 'utf-8').split('\n');
    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx < 0) continue;
        const key = trimmed.slice(0, eqIdx).trim();
        const val = trimmed.slice(eqIdx + 1).trim().replace(/^['"]|['"]$/g, '');
        if (!process.env[key]) process.env[key] = val;
    }
}

const s3 = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
});

async function check() {
    try {
        const res = await s3.send(new HeadObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: 'citi_bike/latest_predictions.parquet'
        }));
        console.log('Last Modified (UTC):', res.LastModified);
    } catch (e) {
        console.error('Error:', e.message);
    }
}
check();
