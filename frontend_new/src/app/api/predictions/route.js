import { NextResponse } from 'next/server';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { parquetRead } from 'hyparquet';

const s3 = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
});

/**
 * Converts a UTC date to US/Eastern timezone string.
 */
function toEastern(dateStr) {
    const d = new Date(dateStr);
    return d.toLocaleString('en-US', { timeZone: 'America/New_York' });
}

/**
 * GET /api/predictions
 * Downloads latest_predictions.parquet from S3, parses, filters future rows,
 * and returns JSON with predictions + metadata.
 */
export async function GET() {
    try {
        const bucket = process.env.AWS_S3_BUCKET;
        if (!bucket) {
            return NextResponse.json(
                { error: 'AWS_S3_BUCKET not configured' },
                { status: 500 }
            );
        }

        // Download parquet from S3
        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: 'citi_bike/latest_predictions.parquet',
        });

        const response = await s3.send(command);
        const bodyBytes = await response.Body.transformToByteArray();

        // Convert Uint8Array → ArrayBuffer, hyparquet requires slice() to return ArrayBuffer
        // Use .buffer with offset correction to avoid subarray pitfalls
        const arrayBuffer = bodyBytes.buffer.slice(
            bodyBytes.byteOffset,
            bodyBytes.byteOffset + bodyBytes.byteLength
        );

        // Parse parquet — file.slice must return ArrayBuffer (not Uint8Array)
        const rows = [];
        await parquetRead({
            file: {
                byteLength: arrayBuffer.byteLength,
                slice: (start, end) => arrayBuffer.slice(start, end),
            },
            onComplete: (data) => {
                for (const row of data) {
                    rows.push(row);
                }
            },
        });


        if (rows.length === 0) {
            return NextResponse.json({ error: 'No data in parquet file' }, { status: 404 });
        }

        // The parquet columns are: target_hour, predicted_trips, prediction_generated_at
        // Determine column order from first row
        const predictions = rows.map((row) => {
            // hyparquet returns arrays, column order matches parquet schema
            const targetHour = row[0]; // timestamp
            const predictedTrips = row[1]; // float
            const generatedAt = row[2]; // timestamp

            return {
                targetHour: typeof targetHour === 'bigint' ? Number(targetHour) : targetHour,
                predictedTrips: typeof predictedTrips === 'bigint' ? Number(predictedTrips) : Number(predictedTrips),
                generatedAt: typeof generatedAt === 'bigint' ? Number(generatedAt) : generatedAt,
            };
        });

        // Convert timestamps and filter to future hours
        const now = new Date();
        const nowEastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));

        const processed = predictions
            .map((p) => {
                // Handle parquet timestamps (could be ms since epoch or Date objects)
                let targetDate;
                if (typeof p.targetHour === 'number') {
                    // If the number looks like microseconds (very large), convert
                    targetDate = p.targetHour > 1e15 ? new Date(p.targetHour / 1000) : new Date(p.targetHour);
                } else if (p.targetHour instanceof Date) {
                    targetDate = p.targetHour;
                } else {
                    targetDate = new Date(p.targetHour);
                }

                let generatedDate;
                if (typeof p.generatedAt === 'number') {
                    generatedDate = p.generatedAt > 1e15 ? new Date(p.generatedAt / 1000) : new Date(p.generatedAt);
                } else if (p.generatedAt instanceof Date) {
                    generatedDate = p.generatedAt;
                } else {
                    generatedDate = new Date(p.generatedAt);
                }

                return {
                    targetHour: targetDate.toISOString(),
                    targetHourET: toEastern(targetDate),
                    predictedTrips: Math.round(p.predictedTrips * 10) / 10,
                    generatedAt: generatedDate.toISOString(),
                    generatedAtET: toEastern(generatedDate),
                };
            })
            .sort((a, b) => new Date(a.targetHour) - new Date(b.targetHour));

        // Filter: only show predictions from current hour onwards
        // If all predictions are in the past, show latest 24 as fallback
        const futureOnly = processed.filter((p) => new Date(p.targetHour) >= now);
        const result = futureOnly.length > 0 ? futureOnly : processed.slice(-24);

        // Compute summary stats
        const peakRow = result.reduce((max, r) => r.predictedTrips > max.predictedTrips ? r : max, result[0]);
        const avgDemand = result.reduce((sum, r) => sum + r.predictedTrips, 0) / result.length;

        // Rush hour detection: hours where demand > 125% of average
        const rushThreshold = avgDemand * 1.25;
        const rushHours = result.filter((r) => r.predictedTrips > rushThreshold);

        return NextResponse.json({
            predictions: result,
            summary: {
                liveForecast: result[0]?.predictedTrips || 0,
                peakDemand: peakRow?.predictedTrips || 0,
                peakHourET: peakRow?.targetHourET || '',
                lastSyncET: result[0]?.generatedAtET || '',
                avgDemand: Math.round(avgDemand * 10) / 10,
                rushHours: rushHours.map((r) => r.targetHourET),
                rushStart: rushHours.length > 0 ? rushHours[0].targetHourET : null,
                rushEnd: rushHours.length > 0 ? rushHours[rushHours.length - 1].targetHourET : null,
            },
        });
    } catch (err) {
        console.error('Predictions API Error:', err);
        return NextResponse.json(
            { error: `Failed to load predictions: ${err.message}` },
            { status: 500 }
        );
    }
}
