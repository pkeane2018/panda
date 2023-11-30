package com.topbloc.codechallenge.db;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DatabaseManager {
    private static final String jdbcPrefix = "jdbc:sqlite:";
    private static final String dbName = "challenge.db";
    private static String connectionString;
    private static Connection conn;

    static {
        File dbFile = new File(dbName);
        connectionString = jdbcPrefix + dbFile.getAbsolutePath();
    }

    public static void connect() {
        try {
            Connection connection = DriverManager.getConnection(connectionString);
            System.out.println("Connection to SQLite has been established.");
            conn = connection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    // Schema function to reset the database if needed - do not change
    public static void resetDatabase() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        File dbFile = new File(dbName);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        connectionString = jdbcPrefix + dbFile.getAbsolutePath();
        connect();
        applySchema();
        seedDatabase();
    }

    // Schema function to reset the database if needed - do not change
    private static void applySchema() {
        String itemsSql = "CREATE TABLE IF NOT EXISTS items (\n"
                + "id integer PRIMARY KEY,\n"
                + "name text NOT NULL UNIQUE\n"
                + ");";
        String inventorySql = "CREATE TABLE IF NOT EXISTS inventory (\n"
                + "id integer PRIMARY KEY,\n"
                + "item integer NOT NULL UNIQUE references items(id) ON DELETE CASCADE,\n"
                + "stock integer NOT NULL,\n"
                + "capacity integer NOT NULL\n"
                + ");";
        String distributorSql = "CREATE TABLE IF NOT EXISTS distributors (\n"
                + "id integer PRIMARY KEY,\n"
                + "name text NOT NULL UNIQUE\n"
                + ");";
        String distributorPricesSql = "CREATE TABLE IF NOT EXISTS distributor_prices (\n"
                + "id integer PRIMARY KEY,\n"
                + "distributor integer NOT NULL references distributors(id) ON DELETE CASCADE,\n"
                + "item integer NOT NULL references items(id) ON DELETE CASCADE,\n"
                + "cost float NOT NULL\n" +
                ");";

        try {
            System.out.println("Applying schema");
            conn.createStatement().execute(itemsSql);
            conn.createStatement().execute(inventorySql);
            conn.createStatement().execute(distributorSql);
            conn.createStatement().execute(distributorPricesSql);
            System.out.println("Schema applied");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // Schema function to reset the database if needed - do not change
    private static void seedDatabase() {
        String itemsSql = "INSERT INTO items (id, name) VALUES (1, 'Licorice'), (2, 'Good & Plenty'),\n"
            + "(3, 'Smarties'), (4, 'Tootsie Rolls'), (5, 'Necco Wafers'), (6, 'Wax Cola Bottles'), (7, 'Circus Peanuts'), (8, 'Candy Corn'),\n"
            + "(9, 'Twix'), (10, 'Snickers'), (11, 'M&Ms'), (12, 'Skittles'), (13, 'Starburst'), (14, 'Butterfinger'), (15, 'Peach Rings'), (16, 'Gummy Bears'), (17, 'Sour Patch Kids')";
        String inventorySql = "INSERT INTO inventory (item, stock, capacity) VALUES\n"
                + "(1, 22, 25), (2, 4, 20), (3, 15, 25), (4, 30, 50), (5, 14, 15), (6, 8, 10), (7, 10, 10), (8, 30, 40), (9, 17, 70), (10, 43, 65),\n" +
                "(11, 32, 55), (12, 25, 45), (13, 8, 45), (14, 10, 60), (15, 20, 30), (16, 15, 35), (17, 14, 60)";
        String distributorSql = "INSERT INTO distributors (id, name) VALUES (1, 'Candy Corp'), (2, 'The Sweet Suite'), (3, 'Dentists Hate Us')";
        String distributorPricesSql = "INSERT INTO distributor_prices (distributor, item, cost) VALUES \n" +
                "(1, 1, 0.81), (1, 2, 0.46), (1, 3, 0.89), (1, 4, 0.45), (2, 2, 0.18), (2, 3, 0.54), (2, 4, 0.67), (2, 5, 0.25), (2, 6, 0.35), (2, 7, 0.23), (2, 8, 0.41), (2, 9, 0.54),\n" +
                "(2, 10, 0.25), (2, 11, 0.52), (2, 12, 0.07), (2, 13, 0.77), (2, 14, 0.93), (2, 15, 0.11), (2, 16, 0.42), (3, 10, 0.47), (3, 11, 0.84), (3, 12, 0.15), (3, 13, 0.07), (3, 14, 0.97),\n" +
                "(3, 15, 0.39), (3, 16, 0.91), (3, 17, 0.85)";

        try {
            System.out.println("Seeding database");
            conn.createStatement().execute(itemsSql);
            conn.createStatement().execute(inventorySql);
            conn.createStatement().execute(distributorSql);
            conn.createStatement().execute(distributorPricesSql);
            System.out.println("Database seeded");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // Helper methods to convert ResultSet to JSON - change if desired, but should not be required
    private static JSONArray convertResultSetToJson(ResultSet rs) throws SQLException{
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<String> colNames = IntStream.range(0, columns)
                .mapToObj(i -> {
                    try {
                        return md.getColumnName(i + 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .collect(Collectors.toList());

        JSONArray jsonArray = new JSONArray();
        while (rs.next()) {
            jsonArray.add(convertRowToJson(rs, colNames));
        }
        return jsonArray;
    }

    private static JSONObject convertRowToJson(ResultSet rs, List<String> colNames) throws SQLException {
        JSONObject obj = new JSONObject();
        for (String colName : colNames) {
            obj.put(colName, rs.getObject(colName));
        }
        return obj;
    }

    // Controller functions - add your routes here. getItems is provided as an example
    public static JSONArray getItems() {
        String sql = "SELECT * FROM items";
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static JSONArray executeQuery(String sql){
        try {
            ResultSet set = conn.createStatement().executeQuery(sql);
            return convertResultSetToJson(set);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static String updateDB(String sql){
        int result = 0;
        try {
            result = conn.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            return e.getMessage();
        }
        if (result > 0){
            return "Status: 200 Success";
        }
        return "Status: 500 Failure";
    }

    // Inventory Methods
    public static JSONArray getAllInventory() {
        String sql = "SELECT items.id, items.name, inventory.stock, inventory.capacity"
                    + " FROM items JOIN inventory"
                    + " ON items.id = inventory.item";
        return executeQuery(sql);
    }

    public static JSONArray getAllOutOfStockItems(){
        String sql = "SELECT items.id, items.name, inventory.stock, inventory.capacity"
                    + " FROM items JOIN inventory ON items.id = inventory.item"
                    + " WHERE inventory.stock <= 0";
        return executeQuery(sql);
    }

    // returns all items for which the quantity currently in stock is less than 35 percent of total capacity
    public static JSONArray getLowStockItems(){
        String sql = "SELECT items.id, items.name, inventory.stock, inventory.capacity"
                    + " FROM items JOIN inventory ON items.id = inventory.item"
                    + " WHERE CAST(inventory.stock as float) / CAST(inventory.capacity as float) < 0.35";
        return executeQuery(sql);
    }

    public static JSONArray getOverStockedItems(){
        String sql = "SELECT items.id, items.name, inventory.stock, inventory.capacity"
                + " FROM items JOIN inventory ON items.id = inventory.item"
                + " WHERE inventory.stock > inventory.capacity";
        return executeQuery(sql);
    }

    public static JSONArray getItemById(String id){
        String sql = "SELECT items.id, items.name, inventory.stock, inventory.capacity"
                + " FROM items JOIN inventory"
                + " ON items.id = inventory.item"
                + " WHERE items.id = " + id;
        return executeQuery(sql);
    }

    public static String addItem(String requestBody){
        JSONParser parser = new JSONParser();
        JSONObject obj;
        String name = "";
        Long id = 0L;
        try {
            obj = (JSONObject) parser.parse(requestBody);
            name = (String) obj.get("name");
            id = (Long) obj.get("id");
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
        String query = "INSERT INTO items (name, id) VALUES ('"+ name + "', " + id + ")";
        return updateDB(query);
    }

    public static String addToInventory(String requestBody){
        JSONParser parser = new JSONParser();
        JSONObject obj;
        Long item = 0L;
        Long stock = 0L;
        Long capacity = 0L;
        try {
            obj = (JSONObject) parser.parse(requestBody);
            item = (Long) obj.get("item");
            stock = (Long) obj.get("stock");
            capacity = (Long) obj.get("capacity");
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
        String query = "INSERT INTO inventory (item, stock, capacity) VALUES (" + item + ", " + stock + ", " + capacity + ")";
        return updateDB(query);
    }

    public static String deleteInventoryItem(String id) {
        String query = "DELETE FROM inventory WHERE inventory.item = " + id;
        return updateDB(query);
    }

    // Distributor Methods
    public static JSONArray getAllDistributors(){
        String sql = "SELECT * FROM distributors";
        return executeQuery(sql);
    }

    // given the provided distributor id, returns all items distributed by the distributor with that id
    public static JSONArray getItemsByDistID(String id){
        String sql = "SELECT items.name, items.id, distributor_prices.cost"
                + " FROM items JOIN distributor_prices ON items.id = distributor_prices.item"
                + " WHERE distributor_prices.distributor = " + id;
        return executeQuery(sql);
    }

    // given the provided item id, returns all offerings from all distributors of the item with that id
    public static JSONArray getDistributorByItemId(String id){
        String sql = "SELECT distributors.name AS distributor_name, distributors.id AS distributors_id, distributor_prices.cost"
                    + " FROM distributors JOIN distributor_prices ON distributors.id = distributor_prices.distributor"
                    + " WHERE distributor_prices.item = " + id;
        return executeQuery(sql);
    }

    public static String addDistributor(String requestBody){
        JSONParser parser = new JSONParser();
        JSONObject obj;
        String name = "";
        Long id = 0L;
        try {
            obj = (JSONObject) parser.parse(requestBody);
            name = (String) obj.get("name");
            id = (Long) obj.get("id");
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
        }
        String query = "INSERT INTO distributors (id, name) VALUES (" + id + ", '" + name + "')";
        return updateDB(query);
    }

    // adds new item to distributor prices table
    public static String addToDistributorCatalogue(String requestBody){
        JSONParser parser = new JSONParser();
        JSONObject obj;
        Long item = 0L;
        Long distributor = 0L;
        String costStr = "";
        try {
            obj = (JSONObject) parser.parse(requestBody);
            item = (Long) obj.get("item");
            distributor = (Long) obj.get("distributor");
            Double cost = (Double) obj.get("cost");
            costStr = String.format("%.2f", cost);
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
        String query = "INSERT INTO distributor_prices (distributor, item, cost) VALUES (" + distributor + ", " + item + ", " + costStr + ")";
        return updateDB(query);
    }

    public static String editItemCost(String itemId, String distributorId, String requestBody){
        JSONParser parser = new JSONParser();
        JSONObject obj;
        String costStr = "";
        try {
            obj = (JSONObject) parser.parse(requestBody);
            Double cost = (Double) obj.get("cost");
            costStr = String.format("%.2f", cost);
        }
        catch (Exception exception){
            System.out.println(exception.getMessage());
        }
        String query = "UPDATE distributor_prices SET cost = " + costStr + " WHERE item = " + itemId + " AND distributor = " + distributorId;
        return updateDB(query);
    }

}
