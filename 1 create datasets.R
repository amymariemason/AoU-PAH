# Creates all the datasets

library(tidyverse)
library(bigrquery)

# all people with EHR and snp data  plus demographics

library(tidyverse)
library(bigrquery)

# This query represents dataset "Everyone demographics" for domain "person" and was generated for All of Us Controlled Tier Dataset v7
dataset_75567458_person_sql <- paste("
    SELECT
        person.person_id,
        person.gender_concept_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        person.race_concept_id,
        p_race_concept.concept_name as race,
        person.ethnicity_concept_id,
        p_ethnicity_concept.concept_name as ethnicity,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id 
    LEFT JOIN
        `concept` p_ethnicity_concept 
            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_whole_genome_variant = 1 ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_ehr_data = 1 ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_75567458_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_75567458",
  "person_75567458_*.csv")
message(str_glue('The data will be written to {person_75567458_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_75567458_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_75567458_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_75567458_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(gender = col_character(), race = col_character(), ethnicity = col_character(), sex_at_birth = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_75567458_person_df <- read_bq_export_from_workspace_bucket(person_75567458_path)



### defining I27 set with condition info
library(tidyverse)
library(bigrquery)

# This query represents dataset "I27" for domain "condition" and was generated for All of Us Controlled Tier Dataset v7
dataset_12328521_condition_sql <- paste("
    SELECT
        c_occurrence.person_id,
        c_occurrence.condition_concept_id,
        c_standard_concept.concept_name as standard_concept_name,
        c_standard_concept.concept_code as standard_concept_code,
        c_standard_concept.vocabulary_id as standard_vocabulary,
        c_occurrence.condition_start_datetime,
        c_occurrence.condition_end_datetime,
        c_occurrence.condition_type_concept_id,
        c_type.concept_name as condition_type_concept_name,
        c_occurrence.stop_reason,
        c_occurrence.visit_occurrence_id,
        visit.concept_name as visit_occurrence_concept_name,
        c_occurrence.condition_source_value,
        c_occurrence.condition_source_concept_id,
        c_source_concept.concept_name as source_concept_name,
        c_source_concept.concept_code as source_concept_code,
        c_source_concept.vocabulary_id as source_vocabulary,
        c_occurrence.condition_status_source_value,
        c_occurrence.condition_status_concept_id,
        c_status.concept_name as condition_status_concept_name 
    FROM
        ( SELECT
            * 
        FROM
            `condition_occurrence` c_occurrence 
        WHERE
            (
                condition_source_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `cb_criteria` cr       
                    WHERE
                        concept_id IN (1326592, 1326593, 1326594, 1326595, 1326596, 1326597, 1326598, 1569150, 35207707, 35207708, 35207709, 35207710, 45533441, 45533442, 45601029)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 0 
                    AND is_selectable = 1)
            )  
            AND (
                c_occurrence.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        criteria.person_id 
                    FROM
                        (SELECT
                            DISTINCT person_id, entry_date, concept_id 
                        FROM
                            `cb_search_all_events` 
                        WHERE
                            (concept_id IN(SELECT
                                DISTINCT c.concept_id 
                            FROM
                                `cb_criteria` c 
                            JOIN
                                (SELECT
                                    CAST(cr.id as string) AS id       
                                FROM
                                    `cb_criteria` cr       
                                WHERE
                                    concept_id IN (1569150)       
                                    AND full_text LIKE '%_rank1]%'      ) a 
                                    ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                    OR c.path LIKE CONCAT('%.', a.id) 
                                    OR c.path LIKE CONCAT(a.id, '.%') 
                                    OR c.path = a.id) 
                            WHERE
                                is_standard = 0 
                                AND is_selectable = 1) 
                            AND is_standard = 0 )) criteria ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `cb_search_person` p 
                    WHERE
                        has_whole_genome_variant = 1 ) 
                    AND cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `cb_search_person` p 
                    WHERE
                        has_ehr_data = 1 ) )
            )) c_occurrence 
    LEFT JOIN
        `concept` c_standard_concept 
            ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
    LEFT JOIN
        `concept` c_type 
            ON c_occurrence.condition_type_concept_id = c_type.concept_id 
    LEFT JOIN
        `visit_occurrence` v 
            ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN
        `concept` visit 
            ON v.visit_concept_id = visit.concept_id 
    LEFT JOIN
        `concept` c_source_concept 
            ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
    LEFT JOIN
        `concept` c_status 
            ON c_occurrence.condition_status_concept_id = c_status.concept_id", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
condition_12328521_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "condition_12328521",
  "condition_12328521_*.csv")
message(str_glue('The data will be written to {condition_12328521_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_12328521_condition_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  condition_12328521_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {condition_12328521_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(standard_concept_name = col_character(), standard_concept_code = col_character(), standard_vocabulary = col_character(), condition_type_concept_name = col_character(), stop_reason = col_character(), visit_occurrence_concept_name = col_character(), condition_source_value = col_character(), source_concept_name = col_character(), source_concept_code = col_character(), source_vocabulary = col_character(), condition_status_source_value = col_character(), condition_status_concept_name = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_12328521_condition_df <- read_bq_export_from_workspace_bucket(condition_12328521_path)

#####  all cause

library(tidyverse)
library(bigrquery)

# This query represents dataset "All cause_ ID only" for domain "person" and was generated for All of Us Controlled Tier Dataset v7
dataset_00597157_person_sql <- paste("
    SELECT
        person.person_id 
    FROM
        `person` person   
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `cb_criteria` cr       
                        WHERE
                            concept_id IN (1326594, 1326592, 1326595, 35207710, 35207707, 1326593, 35207709, 1326597)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 0 
                        AND is_selectable = 1) 
                    AND is_standard = 0 )) criteria ) 
            AND cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT ca.descendant_id 
                    FROM
                        `cb_criteria_ancestor` ca 
                    JOIN
                        (SELECT
                            DISTINCT c.concept_id       
                        FROM
                            `cb_criteria` c       
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id             
                            FROM
                                `cb_criteria` cr             
                            WHERE
                                concept_id IN (1344992, 1337068, 44507580, 44506752, 1354118, 1321636, 35604848, 1327256)             
                                AND full_text LIKE '%_rank1]%'       ) a 
                                ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                OR c.path LIKE CONCAT('%.', a.id) 
                                OR c.path LIKE CONCAT(a.id, '.%') 
                                OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1) b 
                            ON (ca.ancestor_id = b.concept_id)) 
                        AND is_standard = 1)) criteria 
            UNION
            DISTINCT SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT ca.descendant_id 
                    FROM
                        `cb_criteria_ancestor` ca 
                    JOIN
                        (SELECT
                            DISTINCT c.concept_id       
                        FROM
                            `cb_criteria` c       
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id             
                            FROM
                                `cb_criteria` cr             
                            WHERE
                                concept_id IN (1353776, 1336926, 19080432, 1316262, 1328165, 1318853, 1332418, 1318137)             
                                AND full_text LIKE '%_rank1]%'       ) a 
                                ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                OR c.path LIKE CONCAT('%.', a.id) 
                                OR c.path LIKE CONCAT(a.id, '.%') 
                                OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1) b 
                            ON (ca.ancestor_id = b.concept_id)) 
                        AND is_standard = 1)) criteria ) 
                AND cb_search_person.person_id IN (SELECT
                    person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_whole_genome_variant = 1 ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_ehr_data = 1 ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_00597157_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_00597157",
  "person_00597157_*.csv")
message(str_glue('The data will be written to {person_00597157_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_00597157_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_00597157_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_00597157_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- NULL
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_00597157_person_df <- read_bq_export_from_workspace_bucket(person_00597157_path)


# clean PAH 1

library(tidyverse)
library(bigrquery)

# This query represents dataset "Clean_PAH1" for domain "person" and was generated for All of Us Controlled Tier Dataset v7
dataset_73533000_person_sql <- paste("
    SELECT
        person.person_id 
    FROM
        `person` person   
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN (35207707) 
                    AND is_standard = 0 )) criteria ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_ehr_data = 1 ) 
            AND cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT ca.descendant_id 
                    FROM
                        `cb_criteria_ancestor` ca 
                    JOIN
                        (SELECT
                            DISTINCT c.concept_id       
                        FROM
                            `cb_criteria` c       
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id             
                            FROM
                                `cb_criteria` cr             
                            WHERE
                                concept_id IN (1353776, 1336926, 19080432, 1316262, 1328165, 1318853, 1332418, 1318137)             
                                AND full_text LIKE '%_rank1]%'       ) a 
                                ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                OR c.path LIKE CONCAT('%.', a.id) 
                                OR c.path LIKE CONCAT(a.id, '.%') 
                                OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1) b 
                            ON (ca.ancestor_id = b.concept_id)) 
                        AND is_standard = 1)) criteria 
            UNION
            DISTINCT SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT ca.descendant_id 
                    FROM
                        `cb_criteria_ancestor` ca 
                    JOIN
                        (SELECT
                            DISTINCT c.concept_id       
                        FROM
                            `cb_criteria` c       
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id             
                            FROM
                                `cb_criteria` cr             
                            WHERE
                                concept_id IN (1344992, 1337068, 44507580, 44506752, 1354118, 1321636, 35604848, 1327256)             
                                AND full_text LIKE '%_rank1]%'       ) a 
                                ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                                OR c.path LIKE CONCAT('%.', a.id) 
                                OR c.path LIKE CONCAT(a.id, '.%') 
                                OR c.path = a.id) 
                        WHERE
                            is_standard = 1 
                            AND is_selectable = 1) b 
                            ON (ca.ancestor_id = b.concept_id)) 
                        AND is_standard = 1)) criteria ) 
                AND cb_search_person.person_id IN (SELECT
                    person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_whole_genome_variant = 1 ) 
            AND cb_search_person.person_id NOT IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `cb_criteria` cr       
                        WHERE
                            concept_id IN (35206549, 35206836, 35208703, 45586587, 45557101, 19166, 1569120, 45571645, 21254, 1569180, 1553749, 920125, 1326486, 725230, 1569179, 45577767, 21256, 35206784, 45542708, 45594856, 45533457, 45551432, 1567870, 45533456, 35208708, 1567850, 1567844, 35207733, 45552786, 45557540, 35208705, 725223, 45599696, 45606780, 45537942, 35207737, 45604492, 920126, 35211180, 35206767, 35206785, 920127, 35207735, 725235, 1567888, 45567180, 1575689, 35208710, 35208713, 45581267, 35206451, 35208702, 37200584, 35207670, 35208106, 45542710, 45571646, 725224, 45572084, 1569147, 45586576, 45568095, 45591469, 1326484, 45599697, 45571635, 35206456, 45576415, 45576406, 725222, 21255, 45586115, 1567847, 45548022, 45542711, 21250, 45580359, 45575500, 35206725, 45551920, 45567181, 45544120, 45561924, 35208706, 35208711, 1553750, 725237, 45566242, 45595765, 35206782, 35206834, 725219, 35207669, 1567871, 45600606, 35206453, 45552357, 45533441, 45543182, 45585178,
45581266, 1569848, 45548935, 35206837, 725217, 1569149, 1569181, 1567848, 35207729, 35206833, 35206779, 45605789, 1569847, 45600622, 37200585, 45604494, 1567715, 45566702, 1575138, 19165, 19163, 1567889, 35207730, 45586588, 45561925, 45542723, 35207731, 45565786, 45561923, 45601038, 1569148, 35207728, 1575139, 45557100, 45572083, 45551431, 45561926, 725216, 1569156, 35208704, 35224814, 725228, 45604493, 19164, 35208707, 1567849, 45562355, 1575688, 45537454, 1567846, 35206835, 45576878, 35206783, 725234, 45595766, 35206780, 35207736, 45570660, 35208714, 45600621, 19167, 35207734, 1575690, 45601029, 35208709, 1569157, 1567845, 35207792, 45575501, 45581324, 35207732, 45586122, 21257, 35206724, 35206781, 725229, 19168, 45609763)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 0 
                        AND is_selectable = 1) 
                    AND is_standard = 0 )) criteria ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_73533000_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_73533000",
  "person_73533000_*.csv")
message(str_glue('The data will be written to {person_73533000_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_73533000_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_73533000_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_73533000_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- NULL
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_73533000_person_df <- read_bq_export_from_workspace_bucket(person_73533000_path)

library(tidyverse)
library(bigrquery)

# This query represents dataset "Clean_PAH2" for domain "person" and was generated for All of Us Controlled Tier Dataset v7
dataset_20871231_person_sql <- paste("
    SELECT
        person.person_id
    FROM
        `person` person  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                criteria.person_id
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id
                FROM
                    `cb_search_all_events`
                WHERE
                    (concept_id IN (35207707)
                    AND is_standard = 0 )) criteria )
            AND cb_search_person.person_id IN (SELECT
                person_id
            FROM
                `cb_search_person` p
            WHERE
                has_ehr_data = 1 )
            AND cb_search_person.person_id IN (SELECT
                criteria.person_id
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id
                FROM
                    `cb_search_all_events`
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT ca.descendant_id
                    FROM
                        `cb_criteria_ancestor` ca
                    JOIN
                        (SELECT
                            DISTINCT c.concept_id      
                        FROM
                            `cb_criteria` c      
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id            
                            FROM
                                `cb_criteria` cr            
                            WHERE
                                concept_id IN (1353776, 1336926, 19080432, 1316262, 1328165, 1318853, 1332418, 1318137)            
                                AND full_text LIKE '%_rank1]%'       ) a
                                ON (c.path LIKE CONCAT('%.', a.id, '.%')
                                OR c.path LIKE CONCAT('%.', a.id)
                                OR c.path LIKE CONCAT(a.id, '.%')
                                OR c.path = a.id)
                        WHERE
                            is_standard = 1
                            AND is_selectable = 1) b
                            ON (ca.ancestor_id = b.concept_id))
                        AND is_standard = 1)) criteria
            UNION
            DISTINCT SELECT
                criteria.person_id
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id
                FROM
                    `cb_search_all_events`
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT ca.descendant_id
                    FROM
                        `cb_criteria_ancestor` ca
                    JOIN
                        (SELECT
                            DISTINCT c.concept_id      
                        FROM
                            `cb_criteria` c      
                        JOIN
                            (SELECT
                                CAST(cr.id as string) AS id            
                            FROM
                                `cb_criteria` cr            
                            WHERE
                                concept_id IN (1344992, 1337068, 44507580, 44506752, 1354118, 1321636, 35604848, 1327256)            
                                AND full_text LIKE '%_rank1]%'       ) a
                                ON (c.path LIKE CONCAT('%.', a.id, '.%')
                                OR c.path LIKE CONCAT('%.', a.id)
                                OR c.path LIKE CONCAT(a.id, '.%')
                                OR c.path = a.id)
                        WHERE
                            is_standard = 1
                            AND is_selectable = 1) b
                            ON (ca.ancestor_id = b.concept_id))
                        AND is_standard = 1)) criteria )
                AND cb_search_person.person_id IN (SELECT
                    person_id
            FROM
                `cb_search_person` p
            WHERE
                has_whole_genome_variant = 1 )
            AND cb_search_person.person_id NOT IN (SELECT
                criteria.person_id
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id
                FROM
                    `cb_search_all_events`
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id
                    FROM
                        `cb_criteria` c
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id      
                        FROM
                            `cb_criteria` cr      
                        WHERE
                            concept_id IN (35206549, 35206836, 35208703, 45586587, 45557101, 19166, 1569120, 45571645, 21254, 1569180, 1553749, 920125, 1326486, 725230, 1569179, 45577767, 21256, 35206784, 45542708, 45594856, 45533457, 45551432, 1567870, 45533456, 35208708, 1567850, 1567844, 35207733, 45552786, 45557540, 35208705, 725223, 45599696, 45606780, 45537942, 35207737, 45604492, 920126, 35211180, 35206767, 35206785, 920127, 35207735, 725235, 1567888, 45567180, 1575689, 35208710, 35208713, 45581267, 35206451, 35208702, 37200584, 35207670, 35208106, 45542710, 45571646, 725224, 45572084, 1569147, 45586576, 45568095, 45591469, 1326484, 45599697, 45571635, 35206456, 45576415, 45576406, 725222, 21255, 45586115, 1567847, 45548022, 45542711, 21250, 45580359, 45575500, 35206725, 45551920, 45567181, 45544120, 45561924, 35208706, 35208711, 1553750, 725237, 45566242, 45595765, 35206782, 35206834, 725219, 35207669, 1567871, 45600606, 35206453, 45552357, 45533441, 45543182, 45585178,
 45581266, 1569848, 45548935, 35206837, 725217, 1569149, 1569181, 1567848, 35207729, 35206833, 35206779, 45605789, 1569847, 45600622, 37200585, 45604494, 1567715, 45566702, 1575138, 19165, 19163, 1567889, 35207730, 45586588, 45561925, 45542723, 35207731, 45565786, 45561923, 45601038, 1569148, 35207728, 1575139, 45557100, 45572083, 45551431, 45561926, 725216, 1569156, 35208704, 35224814, 725228, 45604493, 19164, 35208707, 1567849, 45562355, 1575688, 45537454, 1567846, 35206835, 45576878, 35206783, 725234, 45595766, 35206780, 35207736, 45570660, 35208714, 45600621, 19167, 35207734, 1575690, 45601029, 35208709, 1569157, 1567845, 35207792, 45575501, 45581324, 35207732, 45586122, 21257, 35206724, 35206781, 725229, 19168, 45609763)      
                            AND full_text LIKE '%_rank1]%'      ) a
                            ON (c.path LIKE CONCAT('%.', a.id, '.%')
                            OR c.path LIKE CONCAT('%.', a.id)
                            OR c.path LIKE CONCAT(a.id, '.%')
                            OR c.path = a.id)
                    WHERE
                        is_standard = 0
                        AND is_selectable = 1)
                    AND is_standard = 0 )) criteria )
            AND cb_search_person.person_id NOT IN (SELECT
                criteria.person_id
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id
                FROM
                    `cb_search_all_events`
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id
                    FROM
                        `cb_criteria` c
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id      
                        FROM
                            `cb_criteria` cr      
                        WHERE
                            concept_id IN (35208013, 35208020, 35208014, 35208016, 35208015, 1569485, 35208017, 35208018, 35208021, 35208023, 1569486, 35208025, 35208019, 35208022, 35208024, 1569487)      
                            AND full_text LIKE '%_rank1]%'      ) a
                            ON (c.path LIKE CONCAT('%.', a.id, '.%')
                            OR c.path LIKE CONCAT('%.', a.id)
                            OR c.path LIKE CONCAT(a.id, '.%')
                            OR c.path = a.id)
                    WHERE
                        is_standard = 0
                        AND is_selectable = 1)
                    AND is_standard = 0 )) criteria ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
person_20871231_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "person_20871231",
  "person_20871231_*.csv")
message(str_glue('The data will be written to {person_20871231_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_20871231_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  person_20871231_path,
  destination_format = "CSV")


# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_20871231_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- NULL
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
dataset_20871231_person_df <- read_bq_export_from_workspace_bucket(person_20871231_path)

