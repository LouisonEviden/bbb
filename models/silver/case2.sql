WITH GetEndDate AS (
    SELECT
        t009.mandt,
        t009.periv,
        t009b.bdatj,
        t009b.bumon,
        t009b.butag,
        t009b.reljr,
        t009b.poper,
        MIN(t009b.bumon) OVER (PARTITION BY t009.mandt, t009.periv, t009b.bdatj) AS MinimumBumon,
        TO_DATE(CONCAT(t009b.bdatj, '-', LPAD(t009b.bumon, 2, '0'), '-', LPAD(t009b.butag, 2, '0')), 'YYYY-MM-DD') AS EndDate
    FROM
        {{ source('source_db', 't009') }} AS t009
    INNER JOIN
        {{ source('source_db', 't009b') }} AS t009b
        ON t009.mandt = t009b.mandt
        AND t009.periv = t009b.periv
    WHERE t009.xjabh = 'X'
        AND t009.mandt = '{{ var("mandt") }}'
),
GetStartDate AS (
    SELECT
        mandt,
        periv,
        bdatj,
        bumon,
        butag,
        reljr,
        poper,
        EndDate,
        COALESCE(
            DATEADD(DAY, 1, LAG(EndDate) OVER (PARTITION BY mandt, periv ORDER BY bdatj, bumon, butag)),
            DATE_TRUNC(MONTH, EndDate)
        ) AS StartDate
    FROM GetEndDate
),
DateSeries AS (
    SELECT
        s.mandt,
        s.periv,
        s.poper,
        s.StartDate,
        s.EndDate,
        s.bdatj,
        s.reljr,
        DATEADD(DAY, seq4(), s.StartDate) AS dt
    FROM GetStartDate AS s,
        TABLE(GENERATOR(ROWCOUNT => 36500)) AS g
    WHERE DATEADD(DAY, seq4(), s.StartDate) <= s.EndDate
),
GetFirstAndLastDay AS (
    SELECT
        mandt,
        periv,
        poper,
        dt,
        MIN(dt) OVER (PARTITION BY mandt, periv, bdatj + reljr) AS FiscalYearFirstDay,
        MAX(dt) OVER (PARTITION BY mandt, periv, bdatj + reljr) AS FiscalYearLastDay,
        DATEADD(DAY, 6 - DAYOFWEEK(dt), dt) AS WeekEndDate,
        DATEADD(DAY, 1 - DAYOFWEEK(dt), dt) AS WeekStartDate,
        bdatj + reljr AS FiscalYear
    FROM DateSeries
),
GetAggFields AS (
    SELECT
        mandt,
        periv,
        dt AS Date,
        FiscalYearFirstDay,
        FiscalYearLastDay,
        FiscalYear AS FiscalYearInt,
        WeekStartDate,
        WeekEndDate,
        poper AS FiscalPeriod,
        CAST(FiscalYear AS STRING) AS FiscalYear,
        CAST(TO_CHAR(dt, 'YYYYMMDD') AS NUMBER) AS DateInt,
        TO_CHAR(dt, 'YYYYMMDD') AS DateStr,
        CASE 
            WHEN dt >= FiscalYearFirstDay AND dt < DATEADD(MONTH, 6, FiscalYearFirstDay) THEN 1
            ELSE 2
        END AS FiscalSemester,
        CASE 
            WHEN dt >= FiscalYearFirstDay AND dt < DATEADD(MONTH, 3, FiscalYearFirstDay) THEN 1
            WHEN dt >= DATEADD(MONTH, 3, FiscalYearFirstDay) AND dt < DATEADD(MONTH, 6, FiscalYearFirstDay) THEN 2
            WHEN dt >= DATEADD(MONTH, 6, FiscalYearFirstDay) AND dt < DATEADD(MONTH, 9, FiscalYearFirstDay) THEN 3
            ELSE 4
        END AS FiscalQuarter,
        CAST(
            CASE 
                WHEN DAYOFWEEK(FiscalYearFirstDay) = 7 THEN CEIL(DATEDIFF(DAY, FiscalYearFirstDay, WeekEndDate) / 7)
                ELSE FLOOR(DATEDIFF(DAY, FiscalYearFirstDay, WeekEndDate) / 7)
            END AS NUMBER
        ) AS FiscalWeek,
        TO_CHAR(dt, 'DAY') AS DayNameLong,
        TO_CHAR(dt, 'DY') AS DayNameShort,
        CONCAT(FiscalYear, poper) AS FiscalYearPeriod
    FROM GetFirstAndLastDay
)
SELECT
    mandt,
    periv,
    Date,
    DateInt,
    DateStr,
    FiscalPeriod,
    FiscalYear,
    FiscalYearPeriod,
    FiscalYearFirstDay,
    FiscalYearLastDay,
    FiscalSemester,
    LPAD(CAST(FiscalSemester AS STRING), 2, '0') AS FiscalSemesterStr,
    CASE WHEN FiscalSemester = 1 THEN '1st Semester' ELSE '2nd Semester' END AS FiscalSemesterStr2,
    FiscalQuarter,
    LPAD(CAST(FiscalQuarter AS STRING), 2, '0') AS FiscalQuarterStr,
    CONCAT('Q', FiscalQuarter) AS FiscalQuarterStr2,
    FiscalWeek,
    LPAD(CAST(FiscalWeek AS STRING), 2, '0') AS FiscalWeekStr,
    WeekStartDate,
    WeekEndDate,
    DayNameLong,
    DayNameShort
FROM GetAggFields