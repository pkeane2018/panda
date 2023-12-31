package com.topbloc.codechallenge;

import com.topbloc.codechallenge.db.DatabaseManager;

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
        get("/version", (req, res) -> "TopBloc Code Challenge v1.0");

        // Item routes
        get("/items", (req, res) -> DatabaseManager.getItems());
        post("/items", (req, res) -> DatabaseManager.addItem(req.body()));

        // Inventory routes
        get("/inventory", (req, res) -> DatabaseManager.getAllInventory());
        get("/inventory/outOfStock", (req, res) -> DatabaseManager.getAllOutOfStockItems());
        get("/inventory/lowStock", (req, res) -> DatabaseManager.getLowStockItems());
        get("/inventory/overStocked", (req, res) -> DatabaseManager.getOverStockedItems());
        get("/inventory/:id", (req, res) -> DatabaseManager.getItemById(req.params("id")));
        post("/inventory", (req, res) -> DatabaseManager.addToInventory(req.body()));
        put("/inventory/:id", (req, res) -> DatabaseManager.updateInventoryItem(req.params("id"), req.body()));
        delete("/inventory/:id", (req, res) -> DatabaseManager.deleteInventoryItem(req.params("id")));

        // Distributor routes
        get("/distributors", (req, res) -> DatabaseManager.getAllDistributors());
        get("/distributors/:itemId", (req, res) -> DatabaseManager.getDistributorByItemId(req.params("itemId")));
        post("/distributors", (req, res) -> DatabaseManager.addDistributor(req.body()));
        delete("/distributors/:id", (req, res) -> DatabaseManager.deleteDistributor(req.params("id")));

        // Distributor Catalog Routes
        get("/catalog/lowestCost/:itemId", (req, res) -> DatabaseManager.getLowestCost(req.params("itemId")));
        get("/catalog/:distributorId", (req, res) -> DatabaseManager.getItemsByDistID(req.params("distributorId")));
        post("/catalog/items/add", (req, res) -> DatabaseManager.addToDistributorCatalog(req.body()));
        patch("/catalog/:itemId/:distributorId", (req, res) -> DatabaseManager.editItemCost(req.params("itemId"), req.params("distributorId"), req.body()));
    }
}