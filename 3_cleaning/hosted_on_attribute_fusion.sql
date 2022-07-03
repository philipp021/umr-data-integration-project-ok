# Creating a dataset with all duplicates:
CREATE TABLE duplicate_rated_movies
(SELECT rated_movies.*
	FROM rated_movies INNER JOIN
		(SELECT counted.imdb_id FROM
			(SELECT imdb_id, COUNT(*) AS count
					FROM rated_movies
                    GROUP BY imdb_id) AS counted
			WHERE count = 2) AS neededIDs
		ON neededIDs.imdb_id = rated_movies.imdb_id);
        
# Fusion of hosted_on attribute:
UPDATE duplicate_rated_movies
SET duplicate_rated_movies.hosted_on = "netflix,prime";

# Remove host_id (not necessary anymore):
ALTER TABLE duplicate_rated_movies
DROP COLUMN host_id;
ALTER TABLE rated_movies
DROP COLUMN host_id;

# Getting all duplicate entries just once:
SELECT * FROM duplicate_rated_movies
GROUP BY imdb_id

# Creating a new table with fusioned hosted_on attribute:  
# First Step (adding data which did not need fusion):
CREATE TABLE fusion_rated_movies
(SELECT rated_movies.*
	FROM rated_movies INNER JOIN
		(SELECT counted.imdb_id FROM
			(SELECT imdb_id, COUNT(*) AS count
					FROM rated_movies
                    GROUP BY imdb_id) AS counted
			WHERE count = 1) AS neededIDs
		ON neededIDs.imdb_id = rated_movies.imdb_id);
# Second Step (adding fusioned data): 
INSERT INTO fusion_rated_movies
(SELECT * FROM duplicate_rated_movies
 GROUP BY imdb_id)
