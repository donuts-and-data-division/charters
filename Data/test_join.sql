

CREATE TABLE result AS (
    SELECT *
    FROM "cleanfilescohort10"
    FULL OUTER JOIN "cleanfilescohort11" 
    ON "CDS10" = "CDS"
    AND "Subgroup10" = "Subgroup11"
    AND "Subgrouptype10" = "Subgrouptype11"
); 


CREATE TABLE result AS (
    SELECT *
    FROM "cleanfilescohort10"
    FULL OUTER JOIN "cleanfilescohort11" 
    ON 'cleanfilescohort10.CDS10' = 'cleanfilescohort11.CDS'
    AND 'cleanfilescohort10.Name10' = 'cleanfilescohort11.Name11'
    AND 'cleanfilescohort10.Subgroup10' = 'cleanfilescohort11.Subgroup11'
    AND 'cleanfilescohort10.Subgrouptype10' = 'cleanfilescohort11.Subgrouptype11'
);




"""
Testing
SELECT *
FROM "cleanfilescohort10"
JOIN "cleanfilescohort11" 
ON 'cleanfilescohort10.CDS10' = 'cleanfilescohort11.CDS'
AND 'cleanfilescohort10.Name10' = 'cleanfilescohort11.Name11'
AND 'cleanfilescohort10.Subgroup10' = 'cleanfilescohort11.Subgroup11'
AND 'cleanfilescohort10.Subgrouptype10' = 'cleanfilescohort11.Subgrouptype11'; 

SELECT COUNT(*)
FROM "cleanfilescohort10"
INNER JOIN "cleanfilescohort11" 
ON "CDS10" = "CDS"
AND "Name10" = "Name11"
AND "Subgroup10" = "Subgroup11"
AND "Subgrouptype10" = "Subgrouptype11"; 


SELECT *
    FROM "cleanfilescohort10"
    INNER JOIN "cleanfilescohort11" 
    ON "Name10" = "Name11";
"""
