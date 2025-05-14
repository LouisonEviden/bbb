-- Copyright 2022 Google LLC
-- Copyright 2023 DataSentics
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

with distribution_channel AS (
    SELECT
        TVTW.MANDT AS Client_MANDT,
        TVTW.VTWEG AS DistributionChannel_VTWEG,
        TVTWT.SPRAS AS Language_SPRAS,
        TVTWT.VTEXT AS DistributionChannelName_VTEXT
    FROM
        {{ source("source_db", "tvtw") }} AS TVTW
    INNER JOIN
        {{ source("source_db", "tvtwt") }} AS TVTWT
        ON
        TVTW.MANDT = TVTWT.MANDT
        AND TVTW.VTWEG = TVTWT.VTWEG
)

SELECT * FROM distribution_channel
