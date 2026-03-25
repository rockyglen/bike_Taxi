import { NextResponse } from 'next/server';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { parquetRead } from 'hyparquet';

async function fetchStationNames(s3, bucket) {
    try {
        const cmd = new GetObjectCommand({ Bucket: bucket, Key: 'citi_bike/monthly_stats.json' });
        const res = await s3.send(cmd);
        const text = await res.Body.transformToString();
        const json = JSON.parse(text);
        return json.topStationNames || {};
    } catch {
        return {};
    }
}

export const dynamic = 'force-dynamic';
export const revalidate = 0;

const s3 = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
});

function toEastern(dateStr) {
    const d = new Date(dateStr);
    return d.toLocaleString('en-US', { timeZone: 'America/New_York' });
}

function resolveTimestamp(val) {
    if (val instanceof Date) return val;
    if (typeof val === 'number') {
        return val > 1e15 ? new Date(val / 1000) : new Date(val);
    }
    return new Date(val);
}

/**
 * GET /api/predictions
 * Downloads latest_predictions.parquet from S3 (multi-station schema):
 *   columns: station_id, station_rank, target_hour, predicted_trips, prediction_generated_at
 * Returns per-station predictions pivoted by hour, plus aggregate summary.
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

        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: 'citi_bike/latest_predictions.parquet',
        });

        const response = await s3.send(command);
        const bodyBytes = await response.Body.transformToByteArray();

        const arrayBuffer = bodyBytes.buffer.slice(
            bodyBytes.byteOffset,
            bodyBytes.byteOffset + bodyBytes.byteLength
        );

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

        // Parquet column order: station_id, station_rank, target_hour, predicted_trips, prediction_generated_at
        const parsed = rows.map((row) => ({
            stationId: String(row[0]),
            stationRank: typeof row[1] === 'bigint' ? Number(row[1]) : Number(row[1]),
            targetHour: resolveTimestamp(typeof row[2] === 'bigint' ? Number(row[2]) : row[2]),
            predictedTrips: Math.round(Number(row[3]) * 10) / 10,
            generatedAt: resolveTimestamp(typeof row[4] === 'bigint' ? Number(row[4]) : row[4]),
        }));

        // Fetch station name mapping from monthly_stats.json (best-effort)
        const stationNames = await fetchStationNames(s3, bucket);

        // Collect unique station IDs ordered by rank
        const stationMap = new Map();
        for (const r of parsed) {
            if (!stationMap.has(r.stationRank)) {
                stationMap.set(r.stationRank, r.stationId);
            }
        }
        const stationIds = [...stationMap.entries()]
            .sort((a, b) => a[0] - b[0])
            .map(([, id]) => id);

        // Pivot: group by targetHour, one entry per hour with a value per station
        const now = new Date();
        const byHour = new Map();

        for (const r of parsed) {
            const key = r.targetHour.toISOString();
            if (!byHour.has(key)) {
                byHour.set(key, {
                    targetHour: r.targetHour.toISOString(),
                    targetHourET: toEastern(r.targetHour),
                    generatedAt: r.generatedAt.toISOString(),
                    generatedAtET: toEastern(r.generatedAt),
                    totalTrips: 0,
                });
            }
            const entry = byHour.get(key);
            entry[r.stationId] = r.predictedTrips;
            entry.totalTrips = Math.round((entry.totalTrips + r.predictedTrips) * 10) / 10;
        }

        let predictions = [...byHour.values()].sort(
            (a, b) => new Date(a.targetHour) - new Date(b.targetHour)
        );

        // Filter to future hours; fallback to last 24
        const futureOnly = predictions.filter((p) => new Date(p.targetHour) >= now);
        predictions = futureOnly.length > 0 ? futureOnly : predictions.slice(-24);

        // Summary based on aggregate (totalTrips)
        const peakRow = predictions.reduce(
            (max, r) => r.totalTrips > max.totalTrips ? r : max,
            predictions[0]
        );
        const avgDemand = predictions.reduce((sum, r) => sum + r.totalTrips, 0) / predictions.length;
        const rushThreshold = avgDemand * 1.25;
        const rushHours = predictions.filter((r) => r.totalTrips > rushThreshold);

        return NextResponse.json({
            stationIds,
            stationNames,
            predictions,
            summary: {
                liveForecast: predictions[0]?.totalTrips || 0,
                peakDemand: peakRow?.totalTrips || 0,
                peakHourET: peakRow?.targetHourET || '',
                lastSyncET: predictions[0]?.generatedAtET || '',
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
