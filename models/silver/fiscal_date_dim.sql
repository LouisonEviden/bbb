WITH
  GetEndDateForFirstScenario AS (
    WITH 
      GeneratedYears AS (
        SELECT 
          YEAR(CURRENT_DATE) - 10 + SEQ4() AS cal_year
        FROM TABLE(GENERATOR(ROWCOUNT => 21))
      ),
      CalculatedPeriods AS (
        SELECT
          t009.mandt,
          t009.periv,
          t009.xkale,
          t009.xjabh,
          gen.cal_year AS cal_year,
          cal_year AS bdatj,
          t009b.bumon,
          t009b.butag,
          t009b.reljr,
          t009b.poper,
          MIN(t009b.bumon) OVER (PARTITION BY t009.mandt, t009.periv) AS MinimumBumon,
          MIN(CONCAT(t009b.poper, '-', t009b.bumon, '-', t009b.butag))
            OVER (PARTITION BY t009.mandt, t009.periv) AS min_period_concat,
          MAX(CONCAT(t009b.poper, '-', t009b.bumon, '-', t009b.butag))
            OVER (PARTITION BY t009.mandt, t009.periv) AS max_period_concat,
          CASE
            WHEN NOT (MOD(gen.cal_year, 4) = 0 AND (MOD(gen.cal_year, 100) != 0 OR MOD(gen.cal_year, 400) = 0))
                AND t009b.bumon = '02' AND t009b.butag = '29'
            THEN TO_DATE(CONCAT(gen.cal_year, '-', t009b.bumon, '-28'))
            ELSE TO_DATE(CONCAT(gen.cal_year, '-', t009b.bumon, '-', t009b.butag))
          END AS Enddate
        FROM
          {{ source('source_db', 't009') }} AS t009
        INNER JOIN
          {{ source('source_db', 't009b') }} AS t009b
          ON t009.mandt = t009b.mandt
          AND t009.periv = t009b.periv
        CROSS JOIN
          GeneratedYears gen
      )
    SELECT
      mandt,
      periv,
      xkale,
      cal_year AS bdatj,
      bumon,
      butag,
      reljr,
      poper,
      MinimumBumon,
      SUBSTR(min_period_concat, 5) AS MinimumPeriod,
      SUBSTR(max_period_concat, 5) AS MaximumPeriod,
      Enddate
    FROM CalculatedPeriods
    WHERE mandt = '{{ var("mandt") }}'
      AND xkale IS NULL
      AND xjabh IS NULL
      AND bdatj = '0000'
  ),
  GetStartDateForFirstScenario AS (
    SELECT
      mandt,
      periv,
      xkale,
      bdatj,
      bumon,
      butag,
      reljr,
      poper,
      EndDate,
      MinimumBumon,
      CAST(bdatj AS NUMBER) + CAST(reljr AS NUMBER) AS FiscalYear,
      MIN(reljr) OVER (PARTITION BY mandt, periv) AS MinimumReljr,
      CASE
        WHEN NOT {{ is_leap_year('bdatj') }}
            AND MinimumPeriod = '02-29'
        THEN REPLACE(MinimumPeriod, '29', '28')
        ELSE MinimumPeriod
      END AS MinimumPeriod,
      CASE
        WHEN NOT {{ is_leap_year('bdatj') }}
            AND MaximumPeriod = '02-29'
        THEN REPLACE(MaximumPeriod, '29', '28')
        ELSE MaximumPeriod
      END AS MaximumPeriod,
      CASE
        WHEN MinimumBumon = 01 THEN
          COALESCE(
            DATEADD('day', 1,
              LAG(EndDate) OVER (PARTITION BY mandt, periv ORDER BY bdatj, bumon, butag)
            ),
            DATE_TRUNC('MONTH', EndDate)
          )
        ELSE
          DATEADD('MONTH', -(MinimumBumon - 1), DATE_TRUNC('MONTH', EndDate))
      END AS StartDate
    FROM GetEndDateForFirstScenario
  ),
  PreGeneratedNumbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
  ),
  DateRanges AS (
    SELECT
        mandt,
        periv,
        poper,
        reljr,
        FiscalYear,
        TO_DATE(StartDate) AS StartDate,
        TO_DATE(EndDate) AS EndDate,
        MinimumBumon,
        MinimumReljr,
        MinimumPeriod,
        MaximumPeriod,
        DATEDIFF('DAY', TO_DATE(StartDate), TO_DATE(EndDate)) + 1 AS DayCount
    FROM GetStartDateForFirstScenario
  ),
  GeneratedDates AS (
      SELECT
          dr.*,
          DATEADD('DAY', pn.seq, dr.StartDate) AS dt
      FROM DateRanges dr
      JOIN PreGeneratedNumbers pn
        ON pn.seq < dr.DayCount
  ),
  GetFirstAndLastDayForFirstScenario AS (
    SELECT
        mandt,
        periv,
        poper,
        dt,
        reljr,
        FiscalYear,
        DATEADD(
            MONTH,
            -(MinimumBumon - 1),
            DATE_TRUNC(
                'MONTH',
                CAST(
                    CASE
                        WHEN MinimumReljr = '0'
                          THEN CONCAT(FiscalYear, '-', MinimumPeriod)
                        WHEN SUBSTR(MinimumReljr, 1, 1) = '+'
                          THEN CONCAT(FiscalYear - CAST(SUBSTR(MinimumReljr, 2, 1) AS NUMBER), '-', MinimumPeriod)
                        WHEN SUBSTR(MinimumReljr, 1, 1) = '-'
                          THEN CONCAT(FiscalYear, '-', MinimumPeriod)
                    END AS DATE)
            )
        ) AS FiscalYearFirstDay,
        CAST(
            CASE
                WHEN MinimumReljr = '0'
                  THEN CONCAT(FiscalYear, '-', MaximumPeriod)
                WHEN SUBSTR(MinimumReljr, 1, 1) = '+'
                  THEN CONCAT(FiscalYear, '-', MaximumPeriod)
                WHEN SUBSTR(MinimumReljr, 1, 1) = '-'
                  THEN CONCAT(FiscalYear + CAST(SUBSTR(MinimumReljr, 2, 1) AS NUMBER), '-', MaximumPeriod)
            END AS DATE
        ) AS FiscalYearLastDay,
        DATE_TRUNC('WEEK', dt) AS WeekStartDate,
        DATEADD('DAY', 6, DATE_TRUNC('WEEK', dt)) AS WeekEndDate
    FROM GeneratedDates
  ),
  GetEndDateForSecondScenario AS (
    SELECT
      t009.mandt,
      t009.periv,
      t009.xkale,
      t009b.bdatj,
      t009b.bumon,
      t009b.butag,
      t009b.reljr,
      t009b.poper,
      MIN(t009b.bumon) OVER (PARTITION BY t009.mandt, t009.periv, t009b.bdatj) AS MinimumBumon,
      TO_DATE(t009b.bdatj || '-' || LPAD(t009b.bumon, 2, '0') || '-' || LPAD(t009b.butag, 2, '0'), 'YYYY-MM-DD') AS EndDate
    FROM
      {{ source('source_db', 't009') }} AS t009
    INNER JOIN
      {{ source('source_db', 't009b') }} AS t009b
      ON
        t009.mandt = t009b.mandt
        AND t009.periv = t009b.periv
    WHERE t009.xkale IS NULL
      AND t009.xjabh IS NULL
      AND t009.mandt = '{{ mandt }}'
      AND t009b.bdatj != '0000'
  ),
  GetStartDateForSecondScenario AS (
    SELECT
      mandt,
      periv,
      bdatj,
      bumon,
      butag,
      reljr,
      poper,
      EndDate,
      CASE 
        WHEN MinimumBumon = 1 THEN 
          COALESCE(
            DATEADD(DAY, 1, LAG(EndDate) OVER (PARTITION BY mandt, periv ORDER BY bdatj, bumon, butag)),
            DATE_TRUNC('MONTH', EndDate))
        ELSE 
          DATEADD(MONTH, -(MinimumBumon - 1), DATE_TRUNC('MONTH', EndDate))
      END AS StartDate
    FROM GetEndDateForSecondScenario
  ),
  GetFirstAndLastDayForSecondScenario AS (
    SELECT
      mandt,
      periv,
      poper,
      dt,
      reljr,
      CAST(bdatj AS NUMBER) + CAST(reljr AS NUMBER) AS FiscalYear,
      MIN(dt) OVER (PARTITION BY mandt, periv, CAST(bdatj AS NUMBER) + CAST(reljr AS NUMBER)) AS FiscalYearFirstDay,
      MAX(dt) OVER (PARTITION BY mandt, periv, CAST(bdatj AS NUMBER) + CAST(reljr AS NUMBER)) AS FiscalYearLastDay,
      DATE_TRUNC('WEEK', dt) AS WeekStartDate,
      DATEADD(DAY, -1, DATEADD(WEEK, 1, DATE_TRUNC('WEEK', dt))) AS WeekEndDate
    FROM (
      SELECT *, 
             DATEADD(DAY, SEQ4() - 1, TO_DATE(StartDate)) AS dt
      FROM GetStartDateForSecondScenario
      QUALIFY SEQ4() <= DATEDIFF(DAY, TO_DATE(StartDate), TO_DATE(EndDate))
    )
  ),
  CombineBothScenarios AS (
  SELECT * FROM GetFirstAndLastDayForFirstScenario
  UNION ALL
  SELECT * FROM GetFirstAndLastDayForSecondScenario
  ),
  GetAggFields AS (
    SELECT
      mandt,
      periv,
      dt AS Date,
      FiscalYearFirstDay,
      FiscalYearLastDay,
      WeekStartDate,
      WeekEndDate,
      poper AS FiscalPeriod,
      CAST(FiscalYear AS STRING) AS FiscalYear,
      CAST(TO_CHAR(dt, 'YYYYMMDD') AS BIGINT) AS DateInt,
      TO_CHAR(dt, 'YYYYMMDD') AS DateStr,
      CASE
        WHEN dt >= FiscalYearFirstDay
          AND dt < DATEADD(MONTH, 6, FiscalYearFirstDay)
          THEN 1
        WHEN dt >= DATEADD(MONTH, 6, FiscalYearFirstDay)
          AND dt <= FiscalYearLastDay
          THEN 2
      END AS FiscalSemester,
      CASE
        WHEN dt >= FiscalYearFirstDay
          AND dt < DATEADD(MONTH, 3, FiscalYearFirstDay)
          THEN 1
        WHEN dt >= DATEADD(MONTH, 3, FiscalYearFirstDay)
          AND dt < DATEADD(MONTH, 6, FiscalYearFirstDay)
          THEN 2
        WHEN dt >= DATEADD(MONTH, 6, FiscalYearFirstDay)
          AND dt < DATEADD(MONTH, 9, FiscalYearFirstDay)
          THEN 3
        WHEN dt >= DATEADD(MONTH, 9, FiscalYearFirstDay)
          AND dt <= FiscalYearLastDay
          THEN 4
      END AS FiscalQuarter,
      CASE
        WHEN TO_CHAR(FiscalYearFirstDay, 'Day') = 'Sunday'
        THEN CEIL(DATEDIFF(DAY, FiscalYearFirstDay, WeekEndDate) / 7)
        ELSE FLOOR(DATEDIFF(DAY, FiscalYearFirstDay, WeekEndDate) / 7)
      END AS FiscalWeek,
      TO_CHAR(dt, 'Day') AS DayNameLong,
      LEFT(TO_CHAR(dt, 'Day'), 3) AS DayNameShort,
      CONCAT(CAST(FiscalYear AS STRING), poper) AS FiscalYearPeriod
    FROM CombineBothScenarios
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
  CASE 
    WHEN FiscalSemester = 1 THEN '1st Semester'
    ELSE '2nd Semester'
  END AS FiscalSemesterStr2,
  FiscalQuarter,
  LPAD(CAST(FiscalQuarter AS STRING), 2, '0') AS FiscalQuarterStr, 
  CONCAT('Q', CAST(FiscalQuarter AS STRING)) AS FiscalQuarterStr2,
  FiscalWeek,
  LPAD(CAST(FiscalWeek AS STRING), 2, '0') AS FiscalWeekStr,
  WeekStartDate,
  WeekEndDate,
  DayNameLong,
  DayNameShort
FROM GetAggFields  
WHERE periv NOT IN ('C2')