CREATE TABLE IF NOT EXISTS trajectory (
        "track_id" NUMERIC REFERENCES vehicle("track_id"),
        "lat" NUMERIC,
        "lon" NUMERIC,
        "speed" NUMERIC,
        "lon_acc" NUMERIC,
        "lat_acc" NUMERIC,
        "time" NUMERIC,
        PRIMARY KEY ("track_id", "time")
);
