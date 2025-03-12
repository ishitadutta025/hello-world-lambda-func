exports.handler = async (event) => {
    try {
        event.Records.forEach(record => {
            console.log("Received SNS message:", record.Sns.Message);
        });
        return { statusCode: 200, body: "SNS message processed" };
    } catch (error) {
        console.error("Error processing SNS message:", error);
        return { statusCode: 500, body: "Error processing SNS message" };
    }
};