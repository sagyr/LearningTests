package com.yotpo.learning;


import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.testcontainers.containers.Neo4jContainer;

import java.time.LocalDateTime;
import java.util.HashMap;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class Neo4JLearningTests {

    private static Neo4jContainer neo4jContainer = null;
    // Retrieve the Bolt URL from the container
    private final String boltUrl = neo4jContainer.getBoltUrl();
    private final String httpUrl = neo4jContainer.getHttpUrl();
//        String boltUrl = "bolt://localhost:7687";

    private final UnomiNeo4JStorage storage = new UnomiNeo4JStorage(boltUrl);


    @BeforeClass
    public static void beforeClass() {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        try {

            neo4jContainer = new Neo4jContainer("neo4j:latest")
                    .withAdminPassword(null); // Disable password
            neo4jContainer.start();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @AfterClass
    public static void afterClass() {
        if (neo4jContainer != null)
            neo4jContainer.stop();
    }

    @Test
    public void exampleOfPastEventsQuery() {


        storage.createProfile("profile1", "il");
        storage.createProfile("profile2", "us");
        storage.createProfile("profile3", "us");
        storage.createProfile("profile4", "il");

        storage.createStore("store1");

        storage.createPurchase("profile1","store1", LocalDateTime.now(), 2200 );
        storage.createPurchase("profile2", "store1", LocalDateTime.of(2021,05, 20, 10, 40), 100.00);
        storage.createPurchase("profile2", "store1", LocalDateTime.of(2021,05, 20, 10, 41), 1900.01);
        storage.createPurchase( "profile3", "store1", LocalDateTime.of(2021,05, 20, 10, 40), 1999.63);
        storage.createPurchase( "profile4", "store1", LocalDateTime.of(2021,05, 20, 10, 40), 2000.63);

//        for (int i = 0; i < 1000; i++) {
//            storage.createPurchase("profile1","store1", LocalDateTime.now().minusDays((i+1)*30), 1000*(i+1) );
//            storage.createPurchase("profile2", "store1", LocalDateTime.of(2021,05, 20, 10, 40).minusDays((i+1)*10), 100.00);
//            storage.createPurchase("profile2", "store1", LocalDateTime.of(2021,05, 20, 10, 41).minusDays((i+1)*10), 190.01);
//            storage.createPurchase( "profile3", "store1", LocalDateTime.of(2021,05, 20, 10, 40).minusDays((i+1)*10), 19.63);
//            storage.createPurchase( "profile4", "store1", LocalDateTime.of(2021,07, 20, 10, 40).minusDays((i+1)*10), 455.63);
//        }


        long startTime = System.currentTimeMillis();
        List<Record> allResults = storage.profilesTotalSpent(
                "store1",
                LocalDateTime.of(2021, 02, 01, 10, 41),
                LocalDateTime.of(2021, 06, 01, 10, 40),
                "il",
                2000.0);
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("query took: %.2f seconds", (endTime-startTime)/1000.0 ));

        assertThat(allResults.size(), is(1));
        Record record1 = allResults.iterator().next();
        Double totalSpent1 = record1.get("totalSpent").asDouble();
        assertThat(totalSpent1, is(2000.63));
        String id = record1.get("p").get("id").asString();
        assertThat(id, is("profile4"));
    }

    public static class UnomiNeo4JStorage {

        private final Session session;

        public UnomiNeo4JStorage(String boltUrl) {
            Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none());
            //Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", "test"));
            session = driver.session();
        }

        public void createPurchase(final String profileID, final String storeID, final LocalDateTime when, final double amount) {
            String createPurchaseQuery = "MATCH (p:Profile), (s:Store)\n" +
                    "WHERE p.id = $profileID and s.id = $storeID\n" +
                    "CREATE (p)-[w:PURCHASED]->(s)\n" +
                    "  SET w.when = datetime($when)\n" +
                    "  SET w.amount = $amount\n" +
                    "RETURN w";

            session.run(createPurchaseQuery, new HashMap<>() {{
                put("profileID", profileID);
                put("storeID", storeID);
                put("when", when);
                put("amount", amount);
            }}).consume();
        }


        public void createStore(final String id) {
            String createStore = "CREATE (s:Store {id:$id})";
            session.run(createStore, new HashMap<>() {{
                put("id", id);
            }}).consume();
        }

        public void createProfile(final String id, final String country) {
            String createProfile = "CREATE (p:Profile {country:$country, id:$id})";
            session.run(createProfile, new HashMap<>() {{
                put("country", country);
                put("id", id);
            }}).consume();
        }

        public List<Record> profilesTotalSpent(String storeID, LocalDateTime after, LocalDateTime before, String country, double moreThan) {
            HashMap<String, Object> params = new HashMap<String, Object>() {{
                put("storeID", storeID);
                put("after", after);
                put("before", before);
                put("moreThan", moreThan);
                put("country", country);
            }};
            List<Record> allResults = this.session.run("CALL {\n" +
                    "  MATCH (p:Profile)-[w:PURCHASED]->(s:Store {id:$storeID})\n" +
                    "  WHERE w.when >= datetime($after) \n" +
                    "      and w.when < datetime($before)\n" +
                    "  return p, sum(w.amount) as totalSpent\n" +
                    "}\n" +
                    "MATCH (p:Profile) \n" +
                    "WHERE totalSpent > $moreThan" +
                    " and p.country = $country\n" +
                    "return p, totalSpent\n", params).list();
            return allResults;
        }
    }
}
