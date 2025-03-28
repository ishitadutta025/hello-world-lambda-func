import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "uuid";

const dynamoDBClient = new DynamoDBClient();
const auditTable = process.env.AUDIT_TABLE;

export const handler = async (event) => {
    try {
        console.log("Received event:", JSON.stringify(event, null, 2));

        for (const record of event.Records) {
            if (record.eventName === 'INSERT') {
                await handleInsertEvent(record);
            } else if (record.eventName === 'MODIFY') {
                await handleModifyEvent(record);
            }
        }

        const response = {
            statusCode: 200,
            body: JSON.stringify('Processed DynamoDB Stream events successfully!'),
        };

        console.log("Final response:", JSON.stringify(response, null, 2));
        return response;

    } catch (error) {
        console.error("Error processing request:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal server error", error: error.message }),
        };
    }
};

const handleInsertEvent = async (record) => {
    const newImage = record.dynamodb.NewImage;

    const itemKey = newImage.key.S;
    const newValue = parseInt(newImage.value.N);
    const modificationTime = new Date().toISOString();

    const auditRecord = {
        id: uuidv4(),
        itemKey: itemKey,
        modificationTime: modificationTime,
        newValue: {
            key: itemKey,
            value: newValue
        },
    };

    console.log("Saving insert event to audit table:", JSON.stringify(auditRecord, null, 2));
    await saveToAuditTable(auditRecord);
};

const handleModifyEvent = async (record) => {
    const newImage = record.dynamodb.NewImage;
    const oldImage = record.dynamodb.OldImage;

    const itemKey = newImage.key.S;
    const newValue = parseInt(newImage.value.N);
    const oldValue = parseInt(oldImage.value.N);
    const modificationTime = new Date().toISOString();

    const auditRecord = {
        id: uuidv4(),
        itemKey: itemKey,
        modificationTime: modificationTime,
        oldValue: oldValue,
        newValue: newValue,
        updatedAttribute: 'value',
    };

    console.log("Saving modify event to audit table:", JSON.stringify(auditRecord, null, 2));
    await saveToAuditTable(auditRecord);
};

const saveToAuditTable = async (auditRecord) => {
    const params = {
        TableName: auditTable,
        Item: auditRecord,
    };

    try {
        console.log("Saving to DynamoDB:", JSON.stringify(params, null, 2));
        const response = await dynamoDBClient.send(new PutCommand(params));
        console.log("Audit record saved successfully:", response);
    } catch (error) {
        console.error("Error saving audit record:", error);
        throw new Error('Error saving audit record');
    }
};
