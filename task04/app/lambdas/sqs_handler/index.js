exports.handler = async (event) => {
    try {
        event.Records.forEach(record => {
            console.log("Received SQS message:", record.body);
        });
        return { statusCode: 200, body: "SQS message processed" };
    } catch (error) {
        console.error("Error processing SQS message:", error);
        return { statusCode: 500, body: "Error processing SQS message" };
    }
};
 