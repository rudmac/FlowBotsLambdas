const DynamoDB = require('aws-sdk/clients/dynamodb');

const BroadcastTableName = process.env.DYNAMODB_TABLE_BROADCAST;
const UseChache = (process.env.USE_CACHE === 'true');
const CacheLiveMinutes = parseInt(process.env.CACHE_LIVE_MINUTES, 10);

const dynamoDbConfig = new DynamoDB({
    maxRetries: 5, // Delays with maxRetries = 5: 30, 60, 120, 240, 480, 920
    retryDelayOptions: {
        base: 30
    },
    httpOptions: {
        timeout: 500
    }
});

const dynamo = new DynamoDB.DocumentClient({
    service: dynamoDbConfig
});

let broadcast_list_cached = undefined;
let cache_datetime = undefined;

async function BroadcastList(db, machine_id) {
    try {
        if (UseChache && cache_datetime !== undefined && (new Date().getTime() - cache_datetime) >= 1000 * 60 * CacheLiveMinutes) { // A cada N minutos
            broadcast_list_cached = undefined;
            console.log("Cache cleared");
        }
        if (UseChache && broadcast_list_cached !== undefined) {
            let cached_list = [];
            for (let i = 0; i < broadcast_list_cached.length; i++) {
                if (broadcast_list_cached[i].broadcast_list_id !== "" && broadcast_list_cached[i].followers_machine_ids.indexOf(machine_id) > -1) {
                    cached_list.push(broadcast_list_cached[i].broadcast_list_id);
                }
            }
            console.log("Return BroadcastList from cache", cached_list);
            return cached_list;
        } else {
            if (UseChache) {
                broadcast_list_cached = [];
            }

            const data = await db.scan({
                TableName: BroadcastTableName,
                ProjectionExpression: "broadcast_list_id, followers_machine_ids"
            }).promise();

            let list = [];
            
            data.Items.forEach(function(element) {
                if (UseChache) {
                    broadcast_list_cached.push({
                        broadcast_list_id: element.broadcast_list_id,
                        followers_machine_ids: element.followers_machine_ids.values
                    });
                }
                if (element.followers_machine_ids.values.indexOf(machine_id) > -1) {
                    list.push(element.broadcast_list_id);
                }
            });
            if (data.Items.length > 0) {
                cache_datetime = new Date().getTime();
            }
            console.log("Return BroadcastList from database", list);
            return list;
        }
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
    //console.log(event);
    //console.log(event.Records);

    for (let i = 0; i < event.Records.length; i++) {
        const record = event.Records[i];

        const dynamobd = record.dynamodb;
        const event_name = record.eventName; // REMOVE and INSERT
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
        } else {
            console.log(event_name, "undefined");
            continue;
        }
        
        const broadcast_list = await BroadcastList(db, machine_id);
        //console.log(broadcast_list);
        if (broadcast_list.length > 0) {
            for (let i = 0; i < broadcast_list.length; i++) {
                const broadcast_list_id = broadcast_list[i];
                if (event_name === "INSERT") {
                    console.log(event_name, broadcast_list_id, connection_id, region);
                    await AddConnectionBroadcastList(db, broadcast_list_id, connection_id, region);
                    //console.log("done");
                } else if (event_name === "REMOVE") {
                    console.log(event_name, broadcast_list_id, connection_id, region);
                    await RemoveConnectionBroadcastList(db, broadcast_list_id, connection_id, region);
                    //console.log("done");
                }
            }
        } else {
            console.log(event_name, "Empty Broadcast List", connection_id, region);
        }
        //console.log(event_name, dynamobd, connection_id, machine_id);
    };

    const response = {
        statusCode: 200,
        body: JSON.stringify({}),
    };
    return response;
};
