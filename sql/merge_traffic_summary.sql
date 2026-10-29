
BEGIN;

INSERT INTO traffic_daily_summary (
    datum,
    source_type,
    kfz_klasse,
    vehicle_count
)
SELECT
    datum,
    source_type,
    kfz_klasse,
    vehicle_count
FROM traffic_daily_summary_staging
ON CONFLICT (datum, source_type, kfz_klasse)
DO UPDATE SET
    vehicle_count = EXCLUDED.vehicle_count,
    updated_at = NOW();

COMMIT;
