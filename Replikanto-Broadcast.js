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
const ConnectDisconnectTableName = process.env.DYNAMODB_TABLE_CONNECT_DISCONNECT;

function Alias(context) {
    const arn = context.invokedFunctionArn;
    //console.log(arn);
    const arnSplited = arn.split(":");
    if (arnSplited.length == 8) {
        const alias = arnSplited[7];
        return alias;
    }
    return undefined;
}

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
        
        return {
            statusCode: 200,
            code: "Success"
        };
    } catch (e) {
        //console.error(e);
        //console.error(data);
        return e;
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
        console.error("RemoveConnectionBroadcastList", error);
    }
}

exports.handler = async (event, context) => {
    //console.log(event);

    let promisses = [];

    const connections = event.connections;
    const action = event.action;
    const payload = event.payload;
    const replikanto_version = event.replikanto_version;
    const broadcast_id = event.broadcast_id;

    console.log("action", action);

    let stage = "production";
    try {
        if (event.requestContext !== undefined) {
            stage = event.requestContext.stage;
        } else { // Aqui vem de uma chamada do BroadcastPositionInfo-Stream
            stage = Alias(context);
        }
        stage = (stage === "prod" || stage === "production") ? "production" : "development";
    } catch (ignored) {
    }
    //console.log("stage", stage);

    for (let i = 0; i < connections.length; i++) {
        const connection = connections[i];
        const connection_obj = JSON.parse(connection);
        //console.log(connection_obj);
        let promise = new Promise(async (resolve, reject) => {
            const ret_obj = await sendToConnection(stage, connection_obj.connection_id, connection_obj.region, {
                action,
                payload,
                replikanto_version,
                broadcast_id
            });
            let ret = {
                connection_id: connection_obj.connection_id,
                region: connection_obj.region,
                status: ret_obj.statusCode
            };
            if (ret.status === 200) {
                console.log(ret);
                resolve(ret);
            } else if (ret.status === 410) { // GoneException
                //console.error(ret);
                
                //Já está sendo removido pelo disconnect da Basics.
                //const broadcast_list_id = event.broadcast_list_id;
                //console.info("Removing", connection_obj.connection_id, "follower from list", broadcast_list_id);
                //await RemoveConnectionBroadcastList(broadcast_list_id, connection_obj.connection_id, connection_obj.region);
                try {
                    const data = await dynamo.query({
                        TableName: ConnectDisconnectTableName,
                        IndexName: "OldConnectionID",
                        ProjectionExpression: "connection_id_new, #region_name",
                        KeyConditionExpression: "connection_id_old = :val1",
                        ExpressionAttributeValues: {
                            ":val1": connection_obj.connection_id,
                            ":val2": new Date((new Date().getTime() - (1 * 60 * 1000)) / 1000).getTime() // até 1 minuto atrás
                        },
                        ExpressionAttributeNames: {
                            "#region_name": "region"
                        },
                        FilterExpression: "disconnection_time >= :val2"
                    }).promise();

                    if (data.Count > 0) {
                        const new_connection_id = data.Items[0]["connection_id_new"];
                        const new_region = data.Items[0]["region"];
                        if (new_connection_id === undefined || new_region == undefined) {
                            console.error(ret_obj, ret_obj.code, `${ConnectDisconnectTableName} has old coonnection but not the new one yet`);
                            reject(ret);
                        } else {
                            const status_code_2_obj = await sendToConnection(stage, new_connection_id, new_region, {
                                action,
                                payload,
                                replikanto_version,
                                broadcast_id
                            });
                            let ret2 = {
                                connection_id: new_connection_id,
                                region: new_region,
                                status: status_code_2_obj.statusCode
                            };
                            if (ret2.status === 200) {
                                console.log(ret2, {connection_id_old: connection_obj.connection_id});
                                resolve(ret2);
                            } else {
                                console.error(ret2, status_code_2_obj.code, `Unable to send data to a new connection ${new_connection_id}`);
                                reject(ret2);
                            }
                        }
                    } else {
                        console.error(ret, ret_obj.code, "Old connection does not persist in the database");
                        reject(ret);
                    }
                } catch (error) {
                    console.error(error);
                    console.error(ret, ret_obj.code);
                }
            } else if (ret.status === 429) { // LimitExceededException
                console.error(ret, ret_obj.code);
                reject(ret);
            } else {
                console.error(ret, ret_obj.code);
                reject(ret);
            }
        });
        promisses.push(promise);
    }
    
    try {
        await Promise.all(promisses);
    } catch (e) {
        //console.error("Check for log errors"); // os erros já vão sair pois tem log lá em cima
    }

    const response = {
        statusCode: 200,
        body: "",
    };
    
    return response;
};