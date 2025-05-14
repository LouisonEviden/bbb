WITH DateSeries AS (
    SELECT 
        DATEADD(DAY, seq4(), DATEADD(YEAR, -10, DATE_TRUNC(YEAR, CURRENT_DATE()))) AS dt
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 3650 * 2))  -- Génère environ 20 ans de dates (10 ans dans chaque direction)
),
FilteredDateSeries AS (
    SELECT
        t009.mandt,
        t009.periv,
        ds.dt
    FROM 
        {{ source('source_db', 't009') }} AS t009
    JOIN 
        DateSeries AS ds
    ON 
        t009.xkale = 'X'
        AND t009.mandt = '{{ var("mandt") }}'
),
FormattedDateSeries AS (
    SELECT
        mandt,
        periv,
        dt AS Date,
        CAST(TO_CHAR(dt, 'YYYYMMDD') AS NUMBER) AS DateInt,
        TO_CHAR(dt, 'YYYYMMDD') AS DateStr,
        TO_CHAR(dt, 'MM') AS FiscalPeriod,
        TO_CHAR(dt, 'YYYY') AS FiscalYear,
        TO_CHAR(dt, 'YYYY') || TO_CHAR(dt, 'MM') AS FiscalYearPeriod,
        DATE_TRUNC(YEAR, dt) AS FiscalYearFirstDay,
        DATEADD(DAY, -1, DATEADD(YEAR, 1, DATE_TRUNC(YEAR, dt))) AS FiscalYearLastDay,
        CASE 
            WHEN EXTRACT(QUARTER FROM dt) IN (1, 2) THEN 1 
            ELSE 2 
        END AS FiscalSemester,
        CASE 
            WHEN EXTRACT(QUARTER FROM dt) IN (1, 2) THEN '01' 
            ELSE '02' 
        END AS FiscalSemesterStr,
        CASE 
            WHEN EXTRACT(QUARTER FROM dt) IN (1, 2) THEN '1st Semester' 
            ELSE '2nd Semester' 
        END AS FiscalSemesterStr2,
        EXTRACT(QUARTER FROM dt) AS FiscalQuarter,
        'Q' || EXTRACT(QUARTER FROM dt) AS FiscalQuarterStr,
        EXTRACT(WEEK FROM dt) AS FiscalWeek,
        TO_CHAR(EXTRACT(WEEK FROM dt), '00') AS FiscalWeekStr,
        DATE_TRUNC(WEEK, dt) AS WeekStartDate,
        DATEADD(DAY, 6, DATE_TRUNC(WEEK, dt)) AS WeekEndDate,
        TO_CHAR(dt, 'Day') AS DayNameLong,
        TO_CHAR(dt, 'DY') AS DayNameShort
    FROM 
        FilteredDateSeries
)

SELECT * FROM FormattedDateSeries