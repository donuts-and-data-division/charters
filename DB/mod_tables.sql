ALTER TABLE "ca_pubschls_new"
ALTER COLUMN "cdscode" TYPE VARCHAR(20);

ALTER TABLE "ca_to_nces"
ALTER COLUMN "NCESCode" TYPE VARCHAR(20),
ALTER COLUMN "CDSCode" TYPE VARCHAR(20);

ALTER TABLE "nces_complete"
ALTER COLUMN "nces_id" TYPE VARCHAR(20);

ALTER TABLE "Alternate_Form_Data_1011"
ADD COLUMN "CDSCode" VARCHAR(20);

UPDATE "Alternate_Form_Data_1011"
SET "CDSCode" = concat("Ccode","Dcode","SchoolID");

ALTER TABLE "Alternate_Form_Data_1112"
ADD COLUMN "CDSCode" VARCHAR(20);

UPDATE "Alternate_Form_Data_1112"
SET "CDSCode" = concat("Ccode","Dcode","SchoolID");

ALTER TABLE "Alternate_Form_Data_1213"
ADD COLUMN "CDSCode" VARCHAR(20);

UPDATE "Alternate_Form_Data_1213"
SET "CDSCode" = concat("Ccode","Dcode","SchoolID");


--combine two nces tables into one nces_complete table
ALTER TABLE nces_download2_new DROP COLUMN IF EXISTS "a";
ALTER TABLE nces_download3_new DROP COLUMN IF EXISTS "a";
ALTER TABLE nces_download2_new RENAME COLUMN "school_id_-_nces_assigned_public_school_latest_available_year" TO "nces_id";
ALTER TABLE nces_download3_new RENAME COLUMN "school_id_-_nces_assigned_public_school_latest_available_year" TO "nces_id";
ALTER TABLE nces_download3_new RENAME COLUMN "school_name" TO "school_name_3";
ALTER TABLE nces_download3_new DROP COLUMN IF EXISTS "state_name_public_school_latest_available_year";

CREATE TABLE nces_complete AS (
    SELECT * FROM nces_download2_new 
    INNER JOIN nces_download3_new USING (nces_id)
    );


--changing pupil/teacher ratio column names to remove slash
ALTER TABLE "nces_complete"
RENAME COLUMN "pupil/teacher_ratio_public_school_2012-13"
TO "pupil_teacher_ratio_2012-13";

ALTER TABLE "nces_complete"
RENAME COLUMN "pupil/teacher_ratio_public_school_2011-12"
TO "pupil_teacher_ratio_2011-12";

ALTER TABLE "nces_complete"
RENAME COLUMN "pupil/teacher_ratio_public_school_2010-11"
TO "pupil_teacher_ratio_2010-11";


--changing asian/pacific_islander columns to remove slash
ALTER TABLE "nces_complete"
RENAME COLUMN "asian_or_pacific_islander_students_public_school_2012-13"
TO "asian_or_pacific_islander_students_public_school_2012-13";

ALTER TABLE "nces_complete"
RENAME COLUMN "asian_or_pacific_islander_students_public_school_2011-12"
TO "asian_or_pacific_islander_students_public_school_2011-12";

ALTER TABLE "nces_complete"
RENAME COLUMN "asian_or_asian/pacific_islander_students_public_school_2010-11"
TO "asian_or_pacific_islander_students_public_school_2010-11";



