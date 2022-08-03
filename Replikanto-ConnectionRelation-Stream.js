const AWS = require('aws-sdk');

const BroadcastTableName = process.env.DYNAMODB_TABLE_BROADCAST;

const dynamoDbConfig = new AWS.DynamoDB({
    maxRetries: 5, // Delays with maxRetries = 5: 30, 60, 120, 240, 480, 920
    retryDelayOptions: {
        base: 30
    },
    httpOptions: {
        timeout: 500
    }
});

const dynamo = new AWS.DynamoDB.DocumentClient({
    service: dynamoDbConfig
});


async function BroadcastList(db, machine_id) {
    try {
        const data = await db.scan({
            TableName: BroadcastTableName,
            ProjectionExpression: "broadcast_list_id",
            FilterExpression: "contains (followers_machine_ids, :val)",
            ExpressionAttributeValues: {
                ":val": machine_id
            },
        }).promise();

        let list = [];
        
        data.Items.forEach(function(element, index, array) {
            list.push(element.broadcast_list_id);
        });

        return list;
    } catch (error) {
        console.error(error);
        return [];
    }
}

async function AddConnectionBroadcastList(db, broadcast_list_id, connection_id, region) {
    try {
        const connectionIDs = db.createSet([JSON.stringify({
            connection_id: connection_id,
            region: region
        })]);
        await db.update({
            TableName: BroadcastTableName,
            Key: { broadcast_list_id },
            ExpressionAttributeValues: { ":var1": connectionIDs },
            UpdateExpression: "add connection_ids :var1",
            ReturnValues: "NONE"
        }).promise();
    } catch (error) {
        console.error(error);
    }
}

async function RemoveConnectionBroadcastList(db, broadcast_list_id, connection_id, region) {
    try {
        const connectionIDs = db.createSet([JSON.stringify({
            connection_id: connection_id,
            region: region
        })]);
        await db.update({
            TableName: BroadcastTableName,
            Key: { broadcast_list_id },
            ExpressionAttributeValues: { ":var1": connectionIDs },
            UpdateExpression: "delete connection_ids :var1",
            ReturnValues: "NONE"
        }).promise();
    } catch (error) {
        console.error(error);
    }
}

exports.handler = async (event) => {
    console.log(event);
    const event_name = event.Records[0].eventName; // REMOVE and INSERT
    const dynamobd = event.Records[0].dynamodb;
    const connection_id = dynamobd.Keys.connection_id.S;
    const db = dynamo;
    
    let region = undefined;
    let machine_id = undefined;
    if (event_name === "REMOVE") {
        machine_id = dynamobd.OldImage.machine_id.S;
        region = dynamobd.OldImage.region.S;
    } else if (event_name === "INSERT") {
        machine_id = dynamobd.NewImage.machine_id.S;
        region = dynamobd.NewImage.region.S;
    }
    
    let broadcast_list = await BroadcastList(db, machine_id);
    //console.log(broadcast_list);
    for (let i = 0; i < broadcast_list.length; i++) {
        const broadcast_list_id = broadcast_list[i];
        if (event_name === "INSERT") {
            console.log(event_name, broadcast_list_id, connection_id);
            await AddConnectionBroadcastList(db, broadcast_list_id, connection_id, region);
            //console.log("done");
        } else if (event_name === "REMOVE") {
            console.log(event_name, broadcast_list_id, connection_id);
            await RemoveConnectionBroadcastList(db, broadcast_list_id, connection_id, region);
            //console.log("done");
        }
    }
    //console.log(event_name, dynamobd, connection_id, machine_id);

    const response = {
        statusCode: 200,
        body: JSON.stringify({}),
    };
    return response;
};
