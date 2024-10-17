const { PutCommand, GetCommand, ScanCommand } = require("@aws-sdk/lib-dynamodb");
const { CreateTableCommand, DescribeTableCommand } = require("@aws-sdk/client-dynamodb");
const { ddbDocClient } = require("./dbconfig");

// Initialize TableRegistry
const initializeTableRegistry = async () => {
  try {
    await ddbDocClient.send(
      new CreateTableCommand({
        TableName: "TableRegistry",
        KeySchema: [{ AttributeName: "tableName", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "tableName", AttributeType: "S" }],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5,
        },
      })
    );
    console.log("TableRegistry created successfully.");
    await waitForTableToBeActive("TableRegistry");
  } catch (error) {
    if (error.name !== "ResourceInUseException") {
      throw error;
    }
  }
};

// Register a table schema
const registerTableSchema = async (tableName, schema) => {
  await initializeTableRegistry();
  
  // Extract field names and types from the first item
  const tableInfo = {
    tableName,
    fields: [
      { name: 'id', type: 'Number', required: true }, // Default id field
      ...Object.entries(schema).map(([key, value]) => ({
        name: key,
        type: typeof value === 'object' ? 
          (Array.isArray(value) ? 'Array' : 'Object') : 
          value.constructor.name,
        required: false
      }))
    ],
    createdAt: new Date().toISOString(),
    lastUpdated: new Date().toISOString()
  };

  await ddbDocClient.send(new PutCommand({
    TableName: "TableRegistry",
    Item: tableInfo
  }));

  return tableInfo;
};

// Get all registered tables and their schemas
const getAllTableSchemas = async () => {
  try {
    const data = await ddbDocClient.send(new ScanCommand({ 
      TableName: "TableRegistry" 
    }));
    return data.Items || [];
  } catch (error) {
    if (error.name === "ResourceNotFoundException") {
      return [];
    }
    throw error;
  }
};

// Get schema for a specific table
const getTableSchema = async (tableName) => {
  try {
    const data = await ddbDocClient.send(new GetCommand({
      TableName: "TableRegistry",
      Key: { tableName }
    }));
    return data.Item;
  } catch (error) {
    if (error.name === "ResourceNotFoundException") {
      return null;
    }
    throw error;
  }
};

module.exports = {
  registerTableSchema,
  getAllTableSchemas,
  getTableSchema,
  initializeTableRegistry
};