const ApiGatewayManagementApi = require('aws-sdk/clients/apigatewaymanagementapi');
const DynamoDB = require('aws-sdk/clients/dynamodb');

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

const BroadcastTableName = process.env.DYNAMODB_TABLE_BROADCAST;

async function sendToConnection(stage, connection_id, region, data) { // parece que é mais lento do que a função antiga
    //let endpoint = requestContext.domainName + '/' + requestContext.stage;
    const wsApiId = process.env["WS_API_ID_" + region.toUpperCase().replace(/-/g, "_")];
    let endpoint = `https://${wsApiId}.execute-api.${region}.amazonaws.com/${stage}`;
    //console.log(endpoint);
    const callbackAPI = new ApiGatewayManagementApi({
        apiVersion: '2018-11-29',
        endpoint: endpoint,
        region
    });

    try {
        await callbackAPI
            .postToConnection({ ConnectionId: connection_id, Data: JSON.stringify(data) })
            .promise();
        
        return 200;
    } catch (e) {
        console.error(connection_id, e.code, e.statusCode);
        return e.statusCode;
    }
}

async function RemoveConnectionBroadcastList(broadcast_list_id, connection_id, region) {
    try {
        const connectionIDs = dynamo.createSet([JSON.stringify({
            connection_id: connection_id,
            region: region
        })]);
        await dynamo.update({
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
    let promisses = [];
    
    const connections = event.connections;
    const action = event.action;
    const payload = event.payload;
    const replikanto_version = event.replikanto_version;
    const broadcast_id = event.broadcast_id;
    const stage = event.requestContext.stage;
    //console.log(event);

    for (let i = 0; i < connections.length; i++) {
        const connection = connections[i];
        const connection_obj = JSON.parse(connection);
        //console.log(connection_obj);
        let promise = new Promise(async (resolve, reject) => {
            const status_code = await sendToConnection(stage, connection_obj.connection_id, connection_obj.region, {
                action,
                payload,
                replikanto_version,
                broadcast_id
            });
            let ret = {
                connection_id: connection_obj.connection_id,
                region: connection_obj.region,
                status: status_code
            };
            if (status_code !== 200) {
                console.error(ret);
                reject(ret);
            } else {
                console.log(ret);
                resolve(ret);
            }
        });
        promisses.push(promise);
    }
    
    try {
        await Promise.all(promisses);
    } catch (e) {
        if (e.status === 410) { // GoneException
            const broadcast_list_id = event.broadcast_list_id;
            console.info("Removing", e.connection_id, "follower from list", broadcast_list_id);
            await RemoveConnectionBroadcastList(broadcast_list_id, e.connection_id, e.region);
        } else {
            console.error("Undefined Error", e, broadcast_list_id);
        }
    }

    const response = {
        statusCode: 200,
        body: "",
    };
    
    return response;
};