DROP TABLE IF EXISTS Schools CASCADE;
CREATE TABLE Schools (
	CDSCode		VARCHAR(13) 	UNIQUE PRIMARY KEY,
	NCESDist	VARCHAR(6),
	NCESSchool 	VARCHAR(5),
	statusType	VARCHAR,
	county      VARCHAR,
	district	VARCHAR,
	school      VARCHAR,
	street		VARCHAR,
	city		VARCHAR,
	zipcode		VARCHAR,
	openDate	DATE,
	closedDate	DATE,
	charter		VARCHAR(1),
	chareterNumber	VARCHAR,
	fundingType		VARCHAR,
	DOC			VARCHAR,
	DOCType		VARCHAR,
	SOCType		VARCHAR,
	edOpsCode	VARCHAR,
	edOpsName	VARCHAR,
	EILCode		VARCHAR,
	EILName		VARCHAR,
	GSOffered	VARCHAR,
	GSServed	VARCHAR,
	virtual		VARCHAR,
	magnet		VARCHAR,
	latitude	NUMERIC,
	longitude	NUMERIC,
	lastUpdated	DATE
);
