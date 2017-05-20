ALTER TABLE nces_download2_new DROP COLUMN "a";
ALTER TABLE nces_download3_new DROP COLUMN "a";
ALTER TABLE nces_download2_new RENAME COLUMN "school_id_-_nces_assigned_public_school_latest_available_year" TO "nces_id";
ALTER TABLE nces_download3_new RENAME COLUMN "school_id_-_nces_assigned_public_school_latest_available_year" TO "nces_id";
ALTER TABLE nces_download3_new DROP COLUMN "school_name" TO "school_name_3";
ALTER TABLE nces_download3_new DROP COLUMN "state_name_public_school_latest_available_year";

CREATE TABLE nces_complete AS 
    (SELECT * FROM nces_download2_new INNER JOIN nces_download3_new USING (nces_id));