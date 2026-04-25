
import csv
import random

COLUMNS = [
    "station_id", "city", "state", "country", "latitude", "longitude", "elevation", "timezone", "observation_date", "observation_time",
    "temp_c", "temp_f", "feels_like_c", "feels_like_f", "dew_point_c", "dew_point_f", "humidity_pct", "pressure_mb", "pressure_in", "visibility_km",
    "visibility_mi", "wind_speed_kph", "wind_speed_mph", "wind_dir_deg", "wind_dir_compass", "wind_gust_kph", "wind_gust_mph", "cloud_cover_pct", "uv_index", "solar_rad_w_m2",
    "precip_mm", "precip_in", "snow_cm", "snow_in", "weather_desc", "weather_code", "icon_url", "is_day", "sunrise_time", "sunset_time",
    "moonrise_time", "moonset_time", "moon_phase", "aqi_us", "aqi_uk", "pm2_5", "pm10", "o3", "no2", "so2",
    "co", "nh3", "heat_index_c", "heat_index_f", "wind_chill_c", "wind_chill_f", "max_temp_c", "max_temp_f", "min_temp_c", "min_temp_f",
    "avg_temp_c", "avg_temp_f", "daily_chance_of_rain_pct", "daily_chance_of_snow_pct", "max_wind_mph", "max_wind_kph", "total_precip_mm", "total_precip_in", "total_snow_cm", "total_snow_in",
    "avg_vis_km", "avg_vis_mi", "avg_humidity_pct", "uv_index_max", "condition_text", "condition_code", "gust_mph_hourly", "gust_kph_hourly", "wind_degree_hourly", "wind_dir_hourly",
    "pressure_mb_hourly", "pressure_in_hourly", "precip_mm_hourly", "precip_in_hourly", "snow_cm_hourly", "snow_in_hourly", "humidity_hourly", "cloud_hourly", "feels_like_c_hourly", "feels_like_f_hourly",
    "windchill_c_hourly", "windchill_f_hourly", "heatindex_c_hourly", "heatindex_f_hourly", "dewpoint_c_hourly", "dewpoint_f_hourly", "will_it_rain_hourly", "chance_of_rain_hourly"
]

def generate_psv(date_str, filename, num_rows=370000):
    print(f"Generating {num_rows} rows for {date_str} into {filename}...")

    base_row = [str(i) for i in range(98)]
    base_row[1] = "Pune"
    base_row[2] = "Maharashtra"
    base_row[3] = "India"
    base_row[8] = date_str
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow(COLUMNS)

        for i in range(num_rows):
            base_row[0] = f"STN-{100000 + i}" 
            base_row[10] = str(random.randint(15, 40)) 
            writer.writerow(base_row)

    print(f"Success! {filename} created.")

if __name__ == "__main__":
    generate_psv("2026-03-24", "weather_20260324.psv", 370000)
    generate_psv("2026-04-18", "weather_20260418.psv", 370000)
