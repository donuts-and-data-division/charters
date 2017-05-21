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
