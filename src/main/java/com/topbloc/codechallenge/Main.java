package com.topbloc.codechallenge;

import com.topbloc.codechallenge.db.DatabaseManager;
import spark.utils.StringUtils;

import static spark.Spark.*;

public class Main {
    public static void main(String[] args) {
        DatabaseManager.connect();
        // Don't change this - required for GET and POST requests with the header 'content-type'
        options("/*",
                (req, res) -> {
                    res.header("Access-Control-Allow-Headers", "content-type");
                    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
                    return "OK";
                });

        // Don't change - if required you can reset your database by hitting this endpoint at localhost:4567/reset
        get("/reset", (req, res) -> {
            DatabaseManager.resetDatabase();
            return "OK";
        });

        //TODO: Add your routes here. a couple of examples are below
        get("/items", (req, res) -> DatabaseManager.getItems());
        get("/version", (req, res) -> "TopBloc Code Challenge v1.0");
        get("/inventory", (req, res) -> {
            // If an id is provided as a query parameter, return data on the item with that id, otherwise return data on all items in inventory
            if (!StringUtils.isEmpty(req.queryParams("id"))){
                return DatabaseManager.getItemById(req.queryParams("id"));
            }
            return DatabaseManager.getAllInventory();
        });
        get("/outOfStock", (req, res) -> DatabaseManager.getAllOutOfStockItems());
        get("/lowStock", (req, res) -> DatabaseManager.getLowStockItems());
        get("/overStocked", (req, res) -> DatabaseManager.getOverStockedItems());
    }
}