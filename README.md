# Concordia's Best Hits (SOEN 363 - Final Project Phase 2)

## Instructions
Please install Neo4j database.

In order to run this project, you need access to download the csv data to be loaded in a Neo4j database.

Please find the `csv.zip` data here: https://drive.google.com/drive/folders/1Ua_iiOlFZQQAFr-20o6xYCsIdQ52Tt4K?usp=sharing

Once the data is downloaded, put all the csv files in the import folder of the Neo4j database.

Run the [`load.cypher`](./neo4j/load.cypher) to load the data in the database. (Warning, the data is quite heavy, and neo4j
might require an excessive amount of RAM.)

Finally, the [`queries.cypher`](./neo4j/queries.cypher) can be evaluated once the data is inserted.

Enjoy!

## Contributors

- Juan-Carlos Sreng-Flores (40101813)
- Walid Achlaf (40210355)
- Daniel Lam (40248073)

## License

This project is licensed under the MIT License. 