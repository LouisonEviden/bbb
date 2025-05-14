WITH Languages_T002 AS (
SELECT
  T002.SPRAS AS LanguageKey_SPRAS,
  T002.LASPEZ AS LanguageSpecifications_LASPEZ,
  T002.LAHQ AS DegreeOfTranslationOfLanguage_LAHQ,
  T002.LAISO AS TwoCharacterSapLanguageCode_LAISO
FROM {{ source("source_db", "t002") }} AS T002
)

SELECT * FROM Languages_T002