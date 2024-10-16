// orgController.js
const { ddbDocClient } = require("./dbconfig");
const { PutCommand, GetCommand, UpdateCommand, DeleteCommand, ScanCommand } = require("@aws-sdk/lib-dynamodb");

// Add an organization
const addOrg = async (orgName, Type, Status, Active) => {
  const params = {
    TableName: "Organizations",
    Item: {
      id: Math.floor(Math.random() * 10000),
      orgName,
      Type,
      Status,
      Active,
      LastUpdated: new Date().toISOString(),
    },
  };
  await ddbDocClient.send(new PutCommand(params));
};

// Get all organizations
const getOrgs = async () => {
  const data = await ddbDocClient.send(new ScanCommand({ TableName: "Organizations" }));
  return data.Items || [];
};

// Get an organization by ID
const getOrg = async (id) => {
  const data = await ddbDocClient.send(new GetCommand({
    TableName: "Organizations",
    Key: { id },
  }));
  if (!data.Item) {
    throw new Error(`Organization with id ${id} not found.`);
  }
  return data.Item;
};

// Update an organization
const updateOrg = async ({ id, orgName, Type, Status, Active }) => {
  const params = {
    TableName: "Organizations",
    Key: { id },
    UpdateExpression: "set orgName = :orgNameVal, #type = :typeVal, #status = :statusVal, Active = :activeVal, LastUpdated = :lastUpdatedVal",
    ExpressionAttributeNames: {
      "#type": "Type",
      "#status": "Status",
    },
    ExpressionAttributeValues: {
      ":orgNameVal": orgName,
      ":typeVal": Type,
      ":statusVal": Status,
      ":activeVal": Active,
      ":lastUpdatedVal": new Date().toISOString(),
    },
  };
  await ddbDocClient.send(new UpdateCommand(params));
};

// Delete an organization
const deleteOrg = async (id) => {
  await ddbDocClient.send(new DeleteCommand({
    TableName: "Organizations",
    Key: { id },
  }));
};

module.exports = { addOrg, getOrgs, getOrg, updateOrg, deleteOrg };