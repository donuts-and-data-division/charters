CREATE TABLE result AS (
    SELECT dropout910.*, 
    	dropout1011.*,
    	dropout1112.*,
    	dropout1213.*,
    	dropout1314.*,
    	dropout1415.*,
    	dropout1516.*,

    FROM clients
    INNER JOIN lce 
    ON clients.client_id = lce.client_id
);