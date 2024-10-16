// server.js
const express = require("express");
const bodyParser = require("body-parser");
const { addOrg, getOrgs, getOrg, updateOrg, deleteOrg } = require("./orgController");
const { authenticateUser } = require("./authController");
const cors = require("cors");

const app = express();
app.use(bodyParser.json());

app.use(cors());

// Or, to allow specific origins, you can use:
app.use(cors({
  origin: "*",// Replace with your Next.js URL
  methods: ["GET", "POST", "PUT", "DELETE"], // Allowed methods
  
}));



app.post("/authenticate", async (req, res) => {
    try {
      const { alias } = req.body; // Get the alias from the request body
  
      // Call the authentication function
      const authData = await authenticateUser(alias);
  
      res.status(200).json(authData); // Return the data if successful
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });

// Route to add an organization
app.post("/org", async (req, res) => {
  try {
    const { orgName, Type, Status, Active } = req.body;
    await addOrg(orgName, Type, Status, Active);
    res.status(201).send("Organization added successfully.");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to get all organizations
app.get("/orgs", async (req, res) => {
  try {
    const orgs = await getOrgs();
    res.status(200).json(orgs);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to get a specific organization by ID
app.get("/org/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const org = await getOrg(parseInt(id));
    res.status(200).json(org);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to update an organization
app.put("/org/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { orgName, Type, Status, Active } = req.body;
    await updateOrg({ id: parseInt(id), orgName, Type, Status, Active });
    res.status(200).send("Organization updated successfully.");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Route to delete an organization
app.delete("/org", async (req, res) => {
  try {
    const { id } = req.body;
    await deleteOrg(parseInt(id));
    res.status(200).send("Organization deleted successfully.");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});