import { NextResponse } from 'next/server';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

const s3 = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
});

export async function GET() {
    try {
        const bucket = process.env.AWS_S3_BUCKET;
        if (!bucket) {
            return NextResponse.json({ error: 'AWS_S3_BUCKET not configured.' }, { status: 500 });
        }

        const cmd = new GetObjectCommand({ Bucket: bucket, Key: 'models/model_metrics.json' });
        const response = await s3.send(cmd);
        const body = await response.Body?.transformToString();
        if (!body) throw new Error('Empty response from S3');
        const metrics = JSON.parse(body);

        return NextResponse.json(metrics);
    } catch (err) {
        if (err.name === 'NoSuchKey') {
            return NextResponse.json(
                { error: 'model_metrics.json not found. Run scripts/train_model.py to generate it.' },
                { status: 404 }
            );
        }
        console.error('model-metrics route error:', err);
        return NextResponse.json({ error: err.message }, { status: 500 });
    }
}
