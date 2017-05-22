CREATE TABLE result AS (
    SELECT cleanCohort2009-2010.*, 
    	cleanCohort2010-11.*,
    	cleanCohort2011-12.*,
    	cleanCohort2012-13.*,
    	cleanCohort2013-14.*,
    	cleanCohort2014-15.*,
    	cleanCohort2015-16.*,

    FROM cleanCohort2009-2010
    OUTER JOIN cleanCohort2010-11 
    ON cleanCohort2009-2010.CDS10 = cleanCohort2010-11.CDS11
    AND cleanCohort2009-2010.Name10 = cleanCohort2010-11.Name11
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


SELECT * FROM "cleanfilescohort10"
UNION ALL
SELECT * FROM "cleanfilescohort11"
ORDER BY "CDS10","Name10", "AggLevel10", "Subgroup10", "Subgrouptype10" limit 10;



    SELECT *
    FROM "cleanfilescohort10"
    JOIN "cleanfilescohort11" 
    ON 'cleanfilescohort10.CDS10' = 'cleanfilescohort11.CDS'
    AND 'cleanfilescohort10.Name10' = 'cleanfilescohort11.Name11'
    AND 'cleanfilescohort10.Subgroup10' = 'cleanfilescohort11.Subgroup11'
    AND 'cleanfilescohort10.Subgrouptype10' = 'cleanfilescohort11.Subgrouptype11'; 


SELECT *
    FROM "cleanfilescohort10"
    INNER JOIN "cleanfilescohort11" 
    ON "Name10" = "Name11";