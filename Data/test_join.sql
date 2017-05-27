
#join two years
CREATE TABLE result AS (
    SELECT *
    FROM "cleanfilescohort10"
    FULL OUTER JOIN "cleanfilescohort11" 
    ON "CDS10" = "CDS"
    AND "Subgroup10" = "Subgroup11"
    AND "Subgrouptype10" = "Subgrouptype11"
); 


#Don't need to do this anymore 
CREATE TABLE dropout AS (
    SELECT "CDS10", "Name10", "AggLevel10", "DFC10", "Subgroup10", "Subgrouptype10", 
    "NumCohort10", "NumGraduates10", "Cohort Graduation Rate10", "NumDropouts10", 
    "Cohort Dropout Rate10", "N"
    FROM "cleanfilescohort10"
    FULL OUTER JOIN "cleanfilescohort11" 
    ON "CDS10" = "CDS11"
    AND "Subgroup10" = "Subgroup11"
    AND "Subgrouptype10" = "Subgrouptype11"
    FULL OUTER JOIN "cleanfilescohort12"
    ON "CDS10" = "CDS12"
    AND "Subgroup10" = "Subgroup12"
    AND "Subgrouptype10" = "Subgrouptype12"
    FULL OUTER JOIN "cleanfilescohort13"
    ON "CDS10" = "CDS13"
    AND "Subgroup10" = "Subgroup13"
    AND "Subgrouptype10" = "Subgrouptype13"
    FULL OUTER JOIN "cleanfilescohort14"
    ON "CDS10" = "CDS14"
    AND "Subgroup10" = "Subgroup14"
    AND "Subgrouptype10" = "Subgrouptype14"
    FULL OUTER JOIN "cleanfilescohort15"
    ON "CDS10" = "CDS15"
    AND "Subgroup10" = "Subgroup15"
    AND "Subgrouptype10" = "Subgrouptype15"
    FULL OUTER JOIN "cleanfilescohort16"
    ON "CDS10" = "CDS16"
    AND "Subgroup10" = "Subgroup16"
    AND "Subgrouptype10" = "Subgrouptype16"
); 



#write query to make long table 
#select * from "dropout_allyears" as A JOIN "dropout_allyears" as B USING ("CDS10");




