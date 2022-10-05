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

async function getConnection(stage, connection_id, region) {
    const wsApiId = process.env["WS_API_ID_" + region.toUpperCase().replace(/-/g, "_")];
    let endpoint = `https://${wsApiId}.execute-api.${region}.amazonaws.com/${stage}`;

    const callbackAPI = new ApiGatewayManagementApi({
        apiVersion: '2018-11-29',
        endpoint: endpoint,
        region
    });

    var params = {
        ConnectionId: connection_id
    };

    try {
        await callbackAPI
            .getConnection(params)
            .promise();
        
        return 200;
    } catch (e) {
        //console.error(endpoint, connection_id, region, e.code, e.statusCode);
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
        console.error("RemoveConnectionBroadcastList", error);
    }
}

exports.handler = async (event, context) => {
    let promisses = [];

    const connections = event.connections;
    const action = event.action;
    const payload = event.payload;
    const replikanto_version = event.replikanto_version;
    const broadcast_id = event.broadcast_id;
    const broadcast_list_id = event.broadcast_list_id;
    const fromLost = event.fromLost; // se vir de um lost, não salva mais...

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
            const data_to_send = {
                action,
                payload,
                replikanto_version,
                broadcast_id,
                broadcast_list_id
            };
            const ret_obj = await sendToConnection(stage, connection_obj.connection_id, connection_obj.region, data_to_send);
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
                try {
                    const data = await dynamo.query({
                        TableName: ConnectDisconnectTableName,
                        IndexName: "OldConnectionID",
                        ProjectionExpression: "connection_id_new, #region_name",
                        KeyConditionExpression: "connection_id_old = :val1",
                        ExpressionAttributeValues: {
                            ":val1": connection_obj.connection_id,
                            ":val2": new Date(new Date().getTime() - (1 * 5 * 1000)).getTime() // até 5 segundos atrás
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
                            try {
                                if (fromLost !== true) {
                                    console.error(ret, ret_obj.code, "Not yet reconnected, it will save lost data for connection event");
                                    const machine_id = data.Items[0]["machine_id"];
                                    const lost_data = [{
                                        "time": new Date().getTime(),
                                        "data": JSON.stringify(data_to_send)
                                    }];
                                    try {
                                        await dynamo.update({
                                            TableName: ConnectDisconnectTableName,
                                            Key: { machine_id },
                                            ExpressionAttributeValues: { ":var1": lost_data, ':empty_list': [] },
                                            UpdateExpression: `set #lost_data = list_append(if_not_exists(#lost_data, :empty_list), :var1)`,
                                            ExpressionAttributeNames: {
                                                '#lost_data': 'lost_data'
                                            },
                                            ReturnValues: "NONE"
                                        }).promise();
                                    } catch (error) {
                                        console.error("Error trying to add lost data to connect/disconnect table for Machine ID", machine_id, lost_data, error);
                                    }
                                } else {
                                    console.error(ret, ret_obj.code, "Not yet reconnected");
                                }
                            } catch (error) {
                                console.error(error);
                            } finally {
                                reject(ret);
                            }
                        } else {
                            try {
                                const tryCount = 10;
                                let tryTimes = 0;
                                while (await getConnection(stage, new_connection_id, new_region) !== 200) {
                                    tryTimes++;
                                    await new Promise(r => setTimeout(r, tryTimes * 50)); // Total 2,75 seconds
                                    if (tryTimes >= tryCount) {
                                        console.log("Connection", new_connection_id, "is not ready to receive trades from lost");
                                        return {
                                            statusCode: 200,
                                            body: "",
                                        };
                                    }
                                    console.log("Try", tryTimes, "time(s)");
                                }
                                console.log("Connection", new_connection_id, "is ready to receive lost data");
                            } catch (error) {
                                console.error("Looping waiting ", new_connection_id, "to be ready");
                            }
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
                                console.error(ret2, status_code_2_obj.code, `Unable to send to new connection ${new_connection_id}`);
                                reject(ret2);
                            }
                        }
                    } else {
                        console.error(ret, ret_obj.code, `Is not persisted in the database or disconnection_time is older`);
                        try {
                            if (broadcast_list_id) {
                                console.info("Removing", connection_obj.connection_id, "from list", broadcast_list_id);
                                await RemoveConnectionBroadcastList(broadcast_list_id, connection_obj.connection_id, connection_obj.region);
                            }
                        } catch (error) {
                            console.error("Removing", connection_obj.connection_id);
                        }
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