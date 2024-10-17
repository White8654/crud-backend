const express = require("express");
const bodyParser = require("body-parser");
const { addItem, getItems, getItem, updateItem, deleteItem } = require("./orgController");
const { authenticateUser } = require("./authController");
const cors = require("cors");

const app = express();
app.use(bodyParser.json());

app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE"],
}));

app.post("/authenticate", async (req, res) => {
  try {
    const { alias } = req.body;
    const authData = await authenticateUser(alias);
    res.status(200).json(authData);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

// Route to add an item
app.post("/item", async (req, res) => {
  try {
    const { tableName, ...itemData } = req.body;
    await addItem(tableName, itemData);
    res.status(201).send("Item added successfully.");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to get all items
app.get("/items", async (req, res) => {
  try {
    const { tableName } = req.body;
    const items = await getItems(tableName);
    res.status(200).json(items);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to get a specific item by ID
app.get("/item/:tableName/:id", async (req, res) => {
  try {
    const { tableName, id } = req.params;
    const item = await getItem(tableName, parseInt(id));
    res.status(200).json(item);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to update an item
app.put("/item/:tableName/:id", async (req, res) => {
  try {
    const { tableName, id } = req.params;
    const updates = req.body;
    await updateItem(tableName, parseInt(id), updates);
    res.status(200).send("Item updated successfully.");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to delete an item
app.delete("/item/:tableName/:id", async (req, res) => {
  try {
    const { tableName, id } = req.params;
    await deleteItem(tableName, parseInt(id));
    res.status(200).send("Item deleted successfully.");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});