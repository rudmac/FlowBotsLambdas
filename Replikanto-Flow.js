"use strict";
// Import the dependency.

const DynamoDB = require('aws-sdk/clients/dynamodb');
const Lambda = require('aws-sdk/clients/lambda');
const ApiGatewayManagementApi = require('aws-sdk/clients/apigatewaymanagementapi');

const region = process.env.AWS_REGION;

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

const lambda = new Lambda({
  apiVersion: "2015-03-31",
  endpoint: `lambda.${region}.amazonaws.com`
});

const ConnectionTableName = process.env.DYNAMODB_TABLE_CONNECTION;
const MachineIDTableName = process.env.DYNAMODB_TABLE_MACHINE_ID;
const BroadcastTableName = process.env.DYNAMODB_TABLE_BROADCAST;
const RetryTimes = process.env.RETRY_TIMES;
const RetryDelay = process.env.RETRY_DELAY;
const ECHO_ID = "@REP-TEST-ECHO";
const OrderStatesToCheck = ["Submitted"/*, "ChangeSubmitted", "CancelSubmitted"*/];
const MachineIDsBlackList = process.env.MACHINE_IDS_BLACK_LIST.split(',');
const BroadcastChuncks = process.env.BROADCAST_CHUNKS;

const re_machine_id = /[0-9A-Fa-f]{32}(-[A-Za-z0-9]{1,60}|)/;
const re_assigned_machine_id = /[0-9A-Fa-f]{32}(-[A-Za-z0-9]{1,60})/;


function cleanMachineID(machine_id) {
    if (machine_id === undefined) {
        throw 'Undefined machine id';
    }
    
    if (machine_id.length < 32) {
        throw `Invalid ${machine_id} machine id`;
    }

    let m;
    if ((m = re_machine_id.exec(machine_id)) !== null) {
        // The result can be accessed through the `m`-variable.
        if (m[0] != machine_id) {
            throw `Invalid ${machine_id} machine id`;
        }
        //m.forEach((match, groupIndex) => {
            //console.log(`Found match, group ${groupIndex}: ${match}`);
        //});
    } else {
        throw `Invalid ${machine_id} machine id`;
    }
    
    return machine_id.slice(0, 32);
}

function cleanAssignedMachineID(machine_id) {
    if (machine_id === undefined) {
        throw 'Undefined machine id';
    }
    
    if (machine_id.length < 32) {
        throw `Invalid ${machine_id} machine id`;
    }

    let m;
    if ((m = re_assigned_machine_id.exec(machine_id)) !== null) {
        // The result can be accessed through the `m`-variable.
        //console.log(m);
        if (m[0] != machine_id) {
            throw `Invalid ${machine_id} machine id`;
        }
        //m.forEach((match, groupIndex) => {
            //console.log(`Found match, group ${groupIndex}: ${match}`);
        //});
    } else {
        throw `Invalid ${machine_id} machine id`;
    }
    
    return {
        MachineId: m[0].slice(0, 32),
        UserDefinedId: m[1].substring(1),
        AssignedMachineId: m.input
    };
}

async function sendToConnection(requestContext, connection_id, region, data) { // parece que é mais lento do que a função antiga
    //let endpoint = requestContext.domainName + '/' + requestContext.stage;
    const wsApiId = process.env["WS_API_ID_" + region.toUpperCase().replace(/-/g, "_")];
    if (region === undefined) {
        region = "us-east-1";
    }
    let stage = requestContext.stage;
    if (stage === "test-invoke-stage") {
        stage = "development";
    }
    let endpoint = `https://${wsApiId}.execute-api.${region}.amazonaws.com/${stage}`;

    const callbackAPI = new ApiGatewayManagementApi({
        apiVersion: '2018-11-29',
        endpoint: endpoint,
        region
    });

    let ret = {};
    try {
        ret = await callbackAPI
            .postToConnection({ ConnectionId: connection_id, Data: JSON.stringify(data) })
            .promise();
        return { status: 200 };
    } catch (e) {
        console.error("sendToConnection Error", connection_id, ret, e);
        return {
            status: e.statusCode,
            statusText: e.code
        };
    }
}

function sleep(millis) {
  return new Promise(resolve => setTimeout(resolve, millis));
}

async function getConnetionID(db, node, myConnection) {
    var params = {
        TableName: ConnectionTableName,
        IndexName: "ReplikantoID",
        ProjectionExpression: "connection_id, #region_name",
        KeyConditionExpression: "replikanto_id = :val",
        ExpressionAttributeValues: {
            ":val": node
        },
        ExpressionAttributeNames: {
            "#region_name": "region"
        }
    };
    
    /* Com a versão nova do Replikanto aonde terá somente uma conexão ws, isso não poderá existir
    if (myConnection !== undefined) {
        params.FilterExpression = "connection_id  <> :my_connection";
        params.ExpressionAttributeValues[":my_connection"] = myConnection;
    }
    */
    
    const data = await db.query(params).promise();
    
    return data;
}

function compareVersion(version, major, minor, build = 0, revision = 0) {
    if (version === undefined || version === "") {
        throw 'Undefined Version Number';
    }
    let versionArr = version.split(".");
    if (versionArr.length < 4) {
        throw 'Undefined Version Number';
    }
    const majorV    = parseInt(versionArr[0], 10);
    const minorV    = parseInt(versionArr[1], 10);
    const buildV    = parseInt(versionArr[2], 10);
    const revisionV = parseInt(versionArr[3], 10);
    
    if (majorV === major) {
        if (minorV === minor) {
            if (buildV === build) {
                if (revisionV === revision) {
                    return 0;
                } else return revisionV - revision;
            } else return buildV - build;
        } else return minorV - minor;
    } else return majorV - major;
}

function IsProd(context) {
    const arn = context.invokedFunctionArn;
    //console.log(arn);
    const arnSplited = arn.split(":");
    let is_Prod = false;
    if (arnSplited.length == 8) {
        const alias = arnSplited[7];
        if (alias === "prod") {
            is_Prod = true;
        }
    }
    return is_Prod;
}

async function FollowersConnectionBroadcastList(db, broadcast_list_id, machine_id) {
    try {
        const data = await db.query({
            TableName: BroadcastTableName,
            KeyConditionExpression: "broadcast_list_id = :val1",
            ProjectionExpression: "connection_ids, broadcast_id",
            FilterExpression: "contains (owner_machine_ids, :val2)",
            ExpressionAttributeValues: {
                ":val1": broadcast_list_id,
                ":val2": machine_id
            },
        }).promise();
        
        //console.log(data);
        
        if (data.Count == 0) {
            return undefined;
        }

        return {
            broadcast_id: data.Items[0]["broadcast_id"],
            connection_ids: ("connection_ids" in data.Items[0] ? data.Items[0]["connection_ids"].values : [])
        };
    } catch (error) {
        console.error(error);
        return undefined;
    }
}

function arrayRemove(arr, value) { 
    return arr.filter(function(ele){ 
        return ele != value; 
    });
}

var functions = [];

functions.onorderupdate = async function(headers, paths, requestContext, body, db, isProd) {
    //console.log('OnOrderUpdate', body);

    const trade = body.trade;
    let nodes = [...new Set(body.nodes)].filter(function (e) { return e != null; });

    if (nodes.length === 0) {
        return {
            action: "node_status",
            credits: {
                has_credits_changed: false,
                credits: 0
            },
            nodes_status: [{
                node: "",
                status: "error",
                msg: "Undefined nodes"
            }]
        };
    } else if (nodes.length === 1 && nodes[0] === "@REP-XXXX-XXXX") {
        return {
            action: "node_status",
            credits: {
                has_credits_changed: false,
                credits: 0
            },
            nodes_status: [{
                node: "@REP-XXXX-XXXX",
                status: "error",
                msg: `Invalid node`
            }]
        };
    }

    let   nodes_retry = [];
    const account = body.account;
    
    let machine_id, assigned_machine_id;
    try {
        assigned_machine_id = cleanAssignedMachineID(body.machine_id);
        machine_id = assigned_machine_id.AssignedMachineId;
    } catch (error) {
        machine_id = cleanMachineID(body.machine_id);
    }
    
    const replikanto_version = body.replikanto_version;
    const replikanto_id = body.replikanto_id;
    
    // BLACKLIST, aqui só quando for via HTTP, porque via WS já faz a verificação durante o connect.
    if (requestContext.hasOwnProperty("http_method")) {
        if (MachineIDsBlackList.includes(machine_id)) {
            //if (nodes.length === 1 && nodes[0] === ECHO_ID) {
                let node_status_bl = [];
                nodes.forEach(function (arrayItem) {
                    node_status_bl.push({
                        node: arrayItem,
                        status: "error",
                        msg: "Your machine id is on the Blacklist"
                    });
                });
                console.log(isProd ? "prod_log" : "dev_log", {
                    trade,
                    account,
                    credits : 0,
                    machine_id,
                    replikanto_id,
                    replikanto_version,
                    //nodes,
                    nodes_status : node_status_bl,
                    elapsed_time: new Date() - Date.parse(trade.timeUTC)
                });
                return {
                    action: "node_status",
                    credits: {
                        has_credits_changed: false,
                        credits: 0
                    },
                    nodes_status: node_status_bl
                };
            //}
        }
    }

    const orderState = trade.orderState;

    let credits = 0;
    let beforeCredit = 0;
    let hasCreditsChanged = false;

    let nodes_status = [];
    let broacast_connections_count = 0;

    const broadcast_list = nodes.filter(function(n) { return n.startsWith("@LST") });
    if (broadcast_list !== undefined && broadcast_list.length > 0) {
        let broadcast_promisses = [];
        const FunctionName = 'Replikanto-Broadcast:' + (isProd ? "prod" : "dev");
        const slice_size = parseInt(BroadcastChuncks, 10);
        await Promise.all(broadcast_list.map(async (broadcast_list_id) => {
            let followersConnections = await FollowersConnectionBroadcastList(db, broadcast_list_id, machine_id);
            if (followersConnections === undefined) {
                return;
            }
            const connections = followersConnections.connection_ids;
            const broadcast_id = followersConnections.broadcast_id;

            broacast_connections_count += connections.length;

            //console.log('Connections', connections);
            console.log('Broadcast ID', broadcast_id);
            console.log(`Invoking ${connections.length} connections with chunck of ${slice_size} for ${broadcast_list_id}`);
            for (let i = 0; i < connections.length; i=i+slice_size) {
                const connections_sliced = connections.slice(i, i+slice_size);
                //console.log('Nodes connections:', connections_sliced);
                console.log(`Slicing chunck ${i+1}-${Math.min(i+slice_size, connections.length)}`);
                broadcast_promisses.push(lambda.invokeAsync({
                    FunctionName,
                    InvokeArgs: JSON.stringify({
                        connections: connections_sliced,
                        action: "trade",
                        payload: trade,
                        replikanto_version,
                        broadcast_id,
                        requestContext
                    })
                }).promise());
            }
        }));


        let broadcast_status = "not broadcast";
        if (broadcast_promisses.length > 0) {
            console.log(`${FunctionName} broadcasting...`);
            await Promise.all(broadcast_promisses);   
            console.log(`${FunctionName} broadcast`);
            broadcast_status = "broadcast";
        }
        
        broadcast_list.map(async (node) => {
            nodes = arrayRemove(nodes, node);
            nodes_status.push({
                node,
                status: broadcast_status
            });
        });
    }

    //if (orderState === "Submitted" || orderState === "ChangeSubmitted" || orderState === "CancelSubmitted") {
    if (OrderStatesToCheck.includes(orderState)) {
        if (!(nodes.length === 1 && nodes[0] === ECHO_ID)) {
            try {
                let nodesToCalc = nodes.filter(item => item !== ECHO_ID);
                let debitedCredit = nodesToCalc.length;
                if (broadcast_list.length > 0) {
                    debitedCredit += broacast_connections_count
                }
                //console.log(debitedCredit);
                //console.log('onorderupdate db.update 1');
                let updateRet = await db.update({
                    TableName: MachineIDTableName,
                    Key: { machine_id },
                    ExpressionAttributeValues: { ":num": debitedCredit },
                    UpdateExpression: "set credits = credits - :num",
                    ConditionExpression: "credits >= :num",
                    ReturnValues: "UPDATED_NEW"
                }).promise();
                //console.log(`Update credits`, updateRet);
                if ("credits" in updateRet.Attributes) {
                    credits = updateRet.Attributes.credits;
                    beforeCredit = credits + debitedCredit;
                } else {
                    credits = 0;
                    beforeCredit = 0;
                }

                hasCreditsChanged = true;
            } catch (error) {
                //ConditionalCheckFailedException tem que zerar
                //console.log("Error", error);
                
                hasCreditsChanged = false;
                credits = 0;
                beforeCredit = 0;
                
                if (error.hasOwnProperty("code")) {
                    if (error.code === "ConditionalCheckFailedException") {
                        //console.log('onorderupdate db.update 2');
                        await db.update({
                            TableName: MachineIDTableName,
                            Key: { machine_id },
                            ExpressionAttributeValues: { ":num": 0 },
                            UpdateExpression: "set credits = :num",
                            ReturnValues: "UPDATED_OLD"
                        }).promise();

                        beforeCredit = 0;
                        hasCreditsChanged = true;
                    }
                }
            }
        }
    }

    let promisses = [];
    let overspend = 0;
    for (var i = 0; i < nodes.length; i++) {
        let node = nodes[i];
        try {
            node = node.toUpperCase();
        } catch (error) {
            // "nodes": [null] conseguiram mandar um node null
            console.log("Error", error);
            continue;
        }

        if (hasCreditsChanged && beforeCredit <= 0) {
            nodes_status.push({
                node,
                status: "error",
                msg: `No credits`
            });
            continue;
        }

        if (OrderStatesToCheck.includes(orderState)) {
            beforeCredit--;
        }

        //console.log(`getConnetionID`);
        //console.log('onorderupdate getConnetionID 1');
        //console.log('my connection', requestContext.connectionId);
        const data = await getConnetionID(db, node, requestContext.connectionId);
        //console.log('my connection', data);
        
        //console.log("items", items);
        if (data.Count > 0) {
            if (node !== ECHO_ID) {
                overspend = overspend + (data.Count - 1);
            }
            const items = data.Items;
            for (const item of items) { // Pode vir mais que 1 conexão, pois pode haver clone da VM e ter o mesmo machineID.
                let promise = new Promise(async (resolve, reject) => {
                    //console.log("item", item);
                    const connection_id = item.connection_id;
                    try {
                        //console.log(`sendToConnection 1 = ${connection_id}`);
                        //console.log('onorderupdate sendToConnection 1');
                        const response = await sendToConnection(requestContext, connection_id, item.region, {
                            action: "trade",
                            payload: trade,
                            replikanto_version
                        });
                        //console.log(response);
                        //console.log(`sendToConnection = ${response.status}`);
                        //console.log(url + "/" + connection_id, response.status);
                        if (response.status === 200) {
                            // mensagem de trade enviada
                            resolve({
                                node,
                                status: 'sent'
                            });
                        } else if (response.status === 403) {
                            //console.log(response);
                            resolve({
                                node,
                                status: "error",
                                msg: response.statusText
                            });
                        } else if (response.status === 504) { // TIMEOUT
                            if (node !== ECHO_ID) {
                                nodes_retry.push(node);
                                resolve({
                                    node,
                                    status: 'retry'
                                });
                            } else {
                                resolve({
                                    node,
                                    status: "error",
                                    msg: response.statusText
                                });
                            }
                        } else {
                            resolve({
                                node,
                                status: "error",
                                msg: response.statusText
                            });
                        }
                    } catch (error) {
                        reject(error);
                    }
                });
                promise.catch((error) => {
                    nodes_status.push({
                        node,
                        status: "error",
                        msg: error
                    });
                });
                promisses.push(promise);
            }
        } else {
            nodes_retry.push(node);
        }
    }

    let responses = await Promise.all(promisses);
    //console.log(`Responses = ${responses}`);
    for(let r of responses) {
        if (r.status === "sent" || r.status === "error") {
            nodes_status.push(r);
        }
        //console.log(`Promise.all = ${r}`);
    }

    if (overspend > 0 && 
            credits > 0 && 
            OrderStatesToCheck.includes(orderState) && 
            compareVersion(replikanto_version, 1, 4, 1, 2) >= 0) { // Aqui é por causa dos safadinhos que tem VM clonada. Isso pode deixar de existir quando eu não deixar novas conexões com o mesmo id
        
        console.log(`${machine_id} overspend ${overspend} credits due to multiple Replikanto Remote ID connections`);
        try {
            let updateRet = await db.update({
                TableName: MachineIDTableName,
                Key: { machine_id },
                ExpressionAttributeValues: { ":num": overspend },
                UpdateExpression: "set credits = credits - :num",
                ConditionExpression: "credits >= :num",
                ReturnValues: "UPDATED_NEW"
            }).promise();
            //console.log(`Update credits`, updateRet);
            credits = updateRet.Attributes.credits;
            beforeCredit = credits + overspend;
            hasCreditsChanged = true;
        } catch (error) {
            //ConditionalCheckFailedException tem que zerar
            //console.log("Error", error);
            if (error.hasOwnProperty("code")) {
                if (error.code === "ConditionalCheckFailedException") {
                    //console.log('onorderupdate db.update 2');
                    let updateRet = await db.update({
                        TableName: MachineIDTableName,
                        Key: { machine_id },
                        ExpressionAttributeValues: { ":num": 0 },
                        UpdateExpression: "set credits = :num",
                        ReturnValues: "UPDATED_OLD"
                    }).promise();
                    hasCreditsChanged = true;
                    credits = 0;
                    beforeCredit = updateRet.Attributes.credits;
                }
            }
        }
    }

    // Retry some nodes after first atempr
    //console.log(`Retry nodes length ${nodes_retry.length}`);
    if (nodes_retry.length > 0) {
        for (let retry = 0; retry < RetryTimes; retry ++) {
            //console.log(`Retry number ${retry + 1}`);
            for (let i = nodes_retry.length - 1; i >= 0; i--) {
                const node = nodes_retry[i];
                //console.log(`Retry node ${node}`);

                //console.log(`Retry sleep ${RetryDelay}`);
                await sleep(RetryDelay);
                
                //console.log('onorderupdate getConnetionID 2');
                const data = await getConnetionID(db, node, requestContext.connectionId);
                console.log(data);
                if (data.Count > 0) {
                    const items = data.Items;
                    for (const item of items) {
                        const connection_id = item.connection_id;
                        if (requestContext.connectionId !== connection_id) {
                            //console.log(`sendToConnection 2 = ${connection_id}`);
                            //console.log('onorderupdate sendToConnection 2');
                            const response = await sendToConnection(requestContext, connection_id, item.region, {
                                action: "trade",
                                payload: trade,
                                replikanto_version
                            });
                            if (response.status === 200) {
                                nodes_status.push({
                                    node,
                                    status: `sent after ${retry + 1} retry(ies)`
                                });
                            } else {
                                nodes_status.push({
                                    node,
                                    status: "error",
                                    msg: response.statusText
                                });
                            }
                            //console.log(`Remove node ${node} from retry`);
                            nodes_retry = nodes_retry.filter(function(value) { 
                                return value !== node;
                            });
                        }
                    }
                }
            }
            
            if (nodes_retry.length == 0) {
                //console.log(`No more nodes to retry`);
                break;
            }
        }
        
        if (nodes_retry.length > 0) {
            for (let i = 0; i < nodes_retry.length; i++) {
                const node = nodes_retry[i];
                nodes_status.push({
                    node,
                    status: "error",
                    msg: `Invalid or disconnected node`
                });
            }
        }
    }
    
    console.log(isProd ? "prod_log" : "dev_log", {
        trade,
        account,
        credits,
        machine_id,
        replikanto_id,
        replikanto_version,
        //nodes,
        nodes_status,
        elapsed_time: new Date() - Date.parse(trade.timeUTC)
    });

    // Para quando for enviado para um remote id com várias conexões, não repetir o status de cada.
    const node_status_reduced = nodes_status.reduce((acc, current) => {
        const found = acc.find(item => (item.node === current.node && item.node === ECHO_ID));
        if (!found)
            return acc.concat([current]);
        else
            return acc;
    }, []);

    return {
        action: "node_status",
        credits: {
            has_credits_changed: hasCreditsChanged,
            credits: credits
        },
        nodes_status: node_status_reduced
    };
};


exports.handler = async (event, context) => {
    try {
        //console.log(event);
        //console.log(context);
        
        const headers = event.headers;
        const paths = event.paths;
        const requestContext = event.requestContext;
        const routeKey = requestContext.routeKey.replace('$','').replace('/','').toLowerCase();
        console.log(`Route Key: ${routeKey}`);
        let body = {};
        try {
            body = JSON.parse(event.body);
        } catch (ignored) {
            body = event.body;
        }

        let statusCode = 200;
        let result = {};
        
        if (!(routeKey in functions)) {
            throw "Invalid function call";
        } else {
            const isProd = IsProd(context);

            //let client;
            //if (isProd) {
                //client = await MongoDB.prod;
            //} else {
                //client = await MongoDB.dev;
            //}
                
            //const db = client.db('Replikanto');
            //const databaseName = db.databaseName;
            //console.log("Database name", databaseName);
            const db = dynamo;
            result = await functions[routeKey](headers, paths, requestContext, body, db, isProd);
            if (result === true) {
                return {
                    statusCode,
                    "isBase64Encoded": false
                };
            } else if (result === false) {
                statusCode = 400;
                return {
                    statusCode,
                    "body": JSON.stringify({
                        payload: {
                            status: "error",
                            msg: "Result was false"
                        }
                    }),
                    "isBase64Encoded": false
                };
            } else { // object
                var response = {
                    statusCode,
                    "body": JSON.stringify(result), // https://aws.amazon.com/pt/premiumsupport/knowledge-center/malformed-502-api-gateway/
                    "isBase64Encoded": false
                };
                return response;
            }
        }
        /*
        if (routeKey === "connect") {
            const response = {
                statusCode
            };
            console.log(response);
            return response;
        } else {
            //console.log('result', result);
            const response = {
                statusCode,
                body: result ? JSON.stringify(result) : "{}",
            };
            console.log(response);
            return response;
        }
        */
    } catch (error) {
        console.log(error, {
            statusCode: 400
        });
        return {
            statusCode: 400,
            "body": JSON.stringify({
                payload: {
                    status: "error",
                    msg: error
                }
            }),
            "isBase64Encoded": false
        };
    }
};