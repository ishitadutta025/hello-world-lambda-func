// exports.handler = async (event) => {
//     // TODO implement
//     const response = {
//         statusCode: 200,
//         body: JSON.stringify('Hello from Lambda!'),
//     };
//     return response;
// };

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "uuid";

// Initialize DynamoDB client
const dynamoDBClient = new DynamoDBClient();
const auditTable = process.env.AUDIT_TABLE;

/**
 * Lambda handler function to process DynamoDB Stream events.
 */
export const handler = async (event) => {
    console.log("Incoming event:", JSON.stringify(event, null, 2));

    try {
        for (const record of event.Records) {
            if (record.eventName === "INSERT") {
                await processInsert(record);
            } else if (record.eventName === "MODIFY") {
                await processModification(record);
            }
        }

        console.log("Event processing completed.");
        return {
            statusCode: 200,
            body: JSON.stringify({ message: "Events processed successfully!" }),
        };
    } catch (error) {
        console.error("Error handling event:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Error processing event", error: error.message }),
        };
    }
};

/**
 * Handles new item insertions in the Configuration table.
 */
const processInsert = async (record) => {
    const newItem = record.dynamodb.NewImage;

    const auditEntry = {
        id: uuidv4(),
        itemKey: newItem.key.S,
        modificationTime: new Date().toISOString(),
        newValue: {
            key: newItem.key.S,
            value: Number(newItem.value.N),
        },
    };

    console.log("Logging insert event:", JSON.stringify(auditEntry, null, 2));
    await saveAuditEntry(auditEntry);
};

/**
 * Handles modifications to existing items in the Configuration table.
 */
const processModification = async (record) => {
    const { NewImage: updatedItem, OldImage: previousItem } = record.dynamodb;

    const auditEntry = {
        id: uuidv4(),
        itemKey: updatedItem.key.S,
        modificationTime: new Date().toISOString(),
        oldValue: Number(previousItem.value.N),
        newValue: Number(updatedItem.value.N),
        updatedAttribute: "value",
    };

    console.log("Logging modification event:", JSON.stringify(auditEntry, null, 2));
    await saveAuditEntry(auditEntry);
};

/**
 * Saves audit log to the Audit table in DynamoDB.
 */
const saveAuditEntry = async (entry) => {
    const params = {
        TableName: auditTable,
        Item: entry,
    };

    try {
        console.log("Writing audit log to DynamoDB:", JSON.stringify(params, null, 2));
        await dynamoDBClient.send(new PutCommand(params));
        console.log("Audit log saved successfully.");
    } catch (error) {
        console.error("Failed to save audit log:", error);
        throw new Error("Could not save audit log entry.");
    }
};
