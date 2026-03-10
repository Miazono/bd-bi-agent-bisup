CREATE TABLE IF NOT EXISTS silver.dim_date (
  date_day DATE,
  date_year INTEGER,
  date_month INTEGER,
  date_day_of_month INTEGER,
  date_day_of_week INTEGER,
  week_of_year INTEGER
)
WITH (
  format = 'PARQUET',
  location = 's3://lakehouse/silver/dim_date/'
);
