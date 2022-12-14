"use strict";
// Import the dependency.
const DynamoDB = require('aws-sdk/clients/dynamodb');
const Lambda = require('aws-sdk/clients/lambda');
const ApiGatewayManagementApi = require('aws-sdk/clients/apigatewaymanagementapi');
const SNS = require('aws-sdk/clients/sns');
const https = require('https');
const agentDynamo = new https.Agent({
    scheduling: 'fifo',
    keepAlive: true,
    timeout: 500
});
const agentApi = new https.Agent({
    scheduling: 'fifo',
    keepAlive: true,
    maxSockets: Infinity,
    timeout: 500
});

const region = process.env.AWS_REGION;

const dynamoDbConfig = new DynamoDB({
    maxRetries: 5, // Delays with maxRetries = 5: 50, 100, 300, 1200, 6000 = 4650 = 7,65s
    retryDelayOptions: {
        base: 50
    },
    httpOptions: {
        agent: agentDynamo,
        timeout: 500 // 0.5s
    },
    region,
    logger: console
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
const TopicAdmMsg = process.env.TOPIC_ADMIN_MESSAGES;
const TopicAdmChatID = process.env.TOPIC_ADMIN_CHAT_ID;
const TopicAdmToken = process.env.TOPIC_ADMIN_TOKEN;

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

async function sendToConnection(requestContext, connection_id, local_region, data, useUnsec = false) { // parece que ?? mais lento do que a fun????o antiga
    //let endpoint = requestContext.domainName + '/' + requestContext.stage;
    if (local_region === undefined) {
        local_region = region;
    }
    let wsApiId = process.env["WS_API_ID_" + local_region.toUpperCase().replace(/-/g, "_")];
    if (useUnsec) {
        wsApiId = process.env.WS_UNSEC_API_ID_US_EAST_1;
    }
    let stage = requestContext.stage;
    if (stage === "test-invoke-stage") {
        stage = "dev";
    }
    let endpoint = `https://${wsApiId}.execute-api.${local_region}.amazonaws.com/${stage}`;

    const callbackAPI = new ApiGatewayManagementApi({
        apiVersion: '2018-11-29',
        endpoint: endpoint,
        region: local_region,
        maxRetries: 5, // Delays with maxRetries = 5: 50, 100, 300, 1200, 6000 = 4650 = 7,65s
        retryDelayOptions: {
            base: 50
        },
        httpOptions: {
            agent: agentApi,
            connectTimeout: 500,
            timeout: 500 // 500ms
        },
        logger: console
    });

    try {
        //console.log("call postToConnection");
        await callbackAPI
            .postToConnection({ ConnectionId: connection_id, Data: JSON.stringify(data) })
            .promise();
        //console.log("finish postToConnection");
        return { status: 200 };
    } catch (e) {
        console.error(connection_id, e.code, e.statusCode);
        //await SNSPublish("Send to Connection", `${connection_id}, ${e.code}, ${e.statusCode}`);
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
        ProjectionExpression: "connection_id, #region_name, replikanto_version",
        KeyConditionExpression: "replikanto_id = :val",
        ExpressionAttributeValues: {
            ":val": node
        },
        ExpressionAttributeNames: {
            "#region_name": "region"
        }
    };
    
    /* Com a vers??o nova do Replikanto aonde ter?? somente uma conex??o ws, isso n??o poder?? existir
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
            ProjectionExpression: "connection_ids, broadcast_id, broadcast_name, telegram_chat_id",
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
            connection_ids: ("connection_ids" in data.Items[0] ? data.Items[0]["connection_ids"].values : []),
            broadcast_name: data.Items[0]["broadcast_name"],
            telegram_chat_id: data.Items[0]["telegram_chat_id"]
        };
    } catch (error) {
        console.error(error);
        await SNSPublish("FollowersConnectionBroadcastList", `${error}`);
        return undefined;
    }
}

function arrayRemove(arr, value) { 
    return arr.filter(function(ele){ 
        return ele != value; 
    });
}

async function SNSPublish(subject, msg, telegram_chat_id = undefined) {
    await new SNS().publish({
        Subject: subject,
        Message: JSON.stringify({
            msg,
            chat_id: telegram_chat_id != undefined ? telegram_chat_id : TopicAdmChatID,
            token: TopicAdmToken
        }),
        TopicArn: TopicAdmMsg,
    }).promise();
}

var functions = [];

functions.onorderupdate = async function(headers, paths, requestContext, body, db, isProd) {
    //console.log('OnOrderUpdate', body);

    const trade = body.trade;
    //console.log("trade", trade);

    let nodes   = [...new Set(body.nodes)].filter(function (e) { return e != null; });
    console.log("nodes", nodes);

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
    console.log("replikanto_id", replikanto_id);
    
    // BLACKLIST, aqui s?? quando for via HTTP, porque via WS j?? faz a verifica????o durante o connect.
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
    console.log("orderState", orderState);

    let credits = 0;
    let beforeCredit = 0;
    let hasCreditsChanged = false;

    let nodes_status = [];
    let broacast_connections_count = 0;

    const broadcast_list = nodes.filter(function(n) { return n.startsWith("@LST") });
    if (broadcast_list !== undefined && broadcast_list.length > 0) {
        let broadcast_promisses = [];
        const FunctionName = 'Replikanto-Broadcast';
        const slice_size = parseInt(BroadcastChuncks, 10);
        await Promise.all(broadcast_list.map(async (broadcast_list_id) => {
            let followersConnections = await FollowersConnectionBroadcastList(db, broadcast_list_id, machine_id);
            if (followersConnections === undefined) {
                return;
            }
            const connections = followersConnections.connection_ids;
            const broadcast_id = followersConnections.broadcast_id;
            const broadcast_name = followersConnections.broadcast_name;
            const telegram_chat_id = followersConnections.telegram_chat_id;

            broacast_connections_count += connections.length;

            //console.log('Connections', connections);
            console.log('Broadcast ID', broadcast_id);
            console.log(`Invoking ${connections.length} connections with chunck of ${slice_size} for ${broadcast_list_id}`);
            for (let i = 0; i < connections.length; i=i+slice_size) {
                const connections_sliced = connections.slice(i, i+slice_size);
                //console.log('Nodes connections:', connections_sliced);
                console.log(`Slicing chunck ${i+1}-${Math.min(i+slice_size, connections.length)}`);
                var params = {
                    FunctionName,
                    //ClientContext: 'STRING_VALUE',
                    InvocationType: "Event",
                    LogType: "None",
                    Payload: JSON.stringify({
                        connections: connections_sliced,
                        action: "trade",
                        payload: trade,
                        replikanto_version,
                        broadcast_list_id,
                        broadcast_id,
                        requestContext
                    }),
                    Qualifier: isProd ? "prod" : "dev"
                };
                broadcast_promisses.push(lambda.invoke(params).promise());
            }
            
            try {
                const tradeName = trade.name ? "\nName: " + trade.name : "";
                const stateName = (trade.orderState == "Filled" && trade.averageFillPrice != undefined) ? trade.orderState + "\nAverage Fill Price: " + trade.averageFillPrice : trade.orderState;
                if (telegram_chat_id !== undefined && ["Cancelled", "Submitted", "Filled", "PartFilled", "Rejected", "ChangeSubmitted"].indexOf(trade.orderState) >= 0) {
                    await SNSPublish("Replikanto Broadcast", `${broadcast_name}\nOrder ${trade.id}\nState: ${stateName}${tradeName}\nAction: ${trade.orderAction}\nInstrument: ${trade.instrument}\nQuantity: ${trade.quantity}\nType: ${trade.orderType}\nLimit Price: ${trade.limitPrice}\nStop Price: ${trade.stopPrice}\nTime: ${trade.time.replace(/T/, ' ').replace(/\..+/, '')}`, telegram_chat_id);
                }
                //if (telegram_chat_id === undefined || TopicAdmChatID != telegram_chat_id) {
                    //await SNSPublish("Broadcast", `${broadcast_name}\nOrder ${trade.id}\nState: ${stateName}${tradeName}\nAction: ${trade.orderAction}\nInstrument: ${trade.instrument}\nQuantity: ${trade.quantity}\nType: ${trade.orderType}\nLimit Price: ${trade.limitPrice}\nStop Price: ${trade.stopPrice}\nTime: ${trade.time.replace(/T/, ' ').replace(/\..+/, '')}`);
                //}
            } catch (error) {
                console.error("" + error);
            }
        }));

        let broadcast_status = "not broadcast"; // se mudar aqui muda l?? em baixo
        if (broadcast_promisses.length > 0) {
            console.log(`${FunctionName} broadcasting...`);
            await Promise.all(broadcast_promisses); 
            console.log(`${FunctionName} broadcast`);
            broadcast_status = "broadcast"; // se mudar aqui muda l?? em baixo
        }
        
        broadcast_list.map(async (node) => {
            nodes = arrayRemove(nodes, node); // Remove dos nodes as listas de broadcasts
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
                if (broadcast_list.length > 0 && false) { // TODO ainda n??o est?? cobrando os cr??ditos para a lista de broadcast
                    debitedCredit += broacast_connections_count;
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
                console.error(error);
                
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
        let data = {
            Count: 0
        };
        try {
            data = await getConnetionID(db, node, requestContext.connectionId);
        } catch (error) {
            console.error("getConnetionID", error.code);
            // vai pro retry
        }
        //console.log('my connection', data);
        
        if (data.Count > 0) {
            console.log(`${node} connection ID found.`);
            const items = data.Items;
            //console.log("items", items);
            for (const item of items) { // Pode vir mais de 1 conex??o, pois pode haver clone da VM e ter o mesmo machineID ou na vers??o 1.5 do Replikanto que tem 2 websocket sobrepostos.
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
                        }, node === ECHO_ID);
                        //console.log(response);
                        //console.log(`sendToConnection = ${response.status}`);
                        //console.log(url + "/" + connection_id, response.status);
                        if (response.status === 200) {
                            // mensagem de trade enviada
                            resolve({
                                node,
                                status: 'sent'
                            });
                        } else if (response.status === 410) { // GoneException
                            if (node !== ECHO_ID && compareVersion(item.replikanto_version, 1, 5, 0, 0) < 0) { // Somente se for nas vers??es antigas onde n??o tem conex??o dupla de WS.
                                nodes_retry.push(node);
                                resolve({
                                    node,
                                    status: 'retry'
                                });
                            } else {
                                if (node !== ECHO_ID) {
                                    // TODO Remove connection id from ConnectionRelation
                                }
                                resolve({
                                    node,
                                    status: "error",
                                    msg: "Invalid or disconnected node"
                                });
                            }
                        } else if (response.status === 504 || response.statusText === "TimeoutError") { // TIMEOUT
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
                                    msg: "Invalid or disconnected node"
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
            // vai tentar mais vezes pois n??o encontrol a conex??o do seguidor
            console.log(`${node} connection ID not found.`);
            nodes_retry.push(node);
        }
    }

    let responses = await Promise.all(promisses);
    console.log("responses", responses);
    
    for(let r of responses) {
        if (r.status === "sent" || r.status === "error") {
            nodes_status.push(r);
        }
        //console.log(`Promise.all = ${r}`);
    }

    // Retry some nodes after first atempr
    if (nodes_retry.length > 0) {
        //console.log(`Retry nodes length ${nodes_retry.length}`);
        for (let retry = 0; retry < RetryTimes; retry ++) {
            console.log(`Retry number ${retry + 1}. Sleeping ${RetryDelay * (retry + 1)}ms before trying again.`);
            //console.log(`Retry sleep ${RetryDelay * (retry + 1)}`);
            await sleep(RetryDelay * (retry + 1));

            for (let i = nodes_retry.length - 1; i >= 0; i--) {
                const node = nodes_retry[i];
                console.log(`Retry node ${node}`);

                const data = await getConnetionID(db, node, requestContext.connectionId);
                if (data.Count > 0) {
                    console.log(`${node} connection ID found.`);
                    const items = data.Items;
                    for (const item of items) {
                        const connection_id = item.connection_id;
                        if (requestContext.connectionId !== connection_id) {
                            const response = await sendToConnection(requestContext, connection_id, item.region, {
                                action: "trade",
                                payload: trade,
                                replikanto_version
                            }, node === ECHO_ID);
                            console.log("Response:", response);
                            if (response.status === 200) {
                                nodes_status.push({
                                    node,
                                    status: `sent after ${retry + 1} retry(ies)`
                                });
                                
                                //console.log(`Remove node ${node} from retry`);
                                nodes_retry = nodes_retry.filter(function(value) { 
                                    return value !== node;
                                });
                            } else if (response.status === 410) { // GoneException
                                if (node !== ECHO_ID) {
                                    // TODO Remove connection id from ConnectionRelation
                                }
                                nodes_status.push({
                                    node,
                                    status: "error",
                                    msg: "Invalid or disconnected node"
                                });
                                // Vai tentar mais vezes
                            } else if (response.status === 504 || response.statusText === "TimeoutError") { // TIMEOUT
                                nodes_status.push({
                                    node,
                                    status: "error",
                                    msg: "Invalid or disconnected node"
                                });
                                // Vai tentar mais vezes
                            } else {
                                nodes_status.push({
                                    node,
                                    status: "error",
                                    msg: "Invalid or disconnected node"
                                });
                                
                                //console.log(`Remove node ${node} from retry`);
                                nodes_retry = nodes_retry.filter(function(value) { 
                                    return value !== node;
                                });
                            }
                        }
                    }
                } else {
                    console.log("Connection ID not found.");
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
                    msg: "Invalid or disconnected node"
                });
            }
        }
    }
    
    // Para quando for enviado para um remote id com v??rias conex??es, n??o repetir o status de cada.
    const unique_ids = [...new Set(nodes_status.map(o => o.node))].filter(function (e) { return e != null; });
    const node_status_reduced = [];
    for (let i = 0; i < unique_ids.length; i++) {
        let found = false;
        for (let e = 0; e < nodes_status.length; e++) {
            let node_status_local = nodes_status[e];
            if (node_status_local.node == unique_ids[i] && (node_status_local.status.startsWith("sent") || node_status_local.status === "broadcast")) {
                found = true;
                node_status_reduced.push(node_status_local);
                break;
            }
        }
        if (!found) {
            let node = nodes_status.filter((n) => n.node === unique_ids[i] && (n.status === "error" || n.status === "not broadcast"))[0];
            if (node === undefined) {
                node = {
                    node: unique_ids[i],
                    status: "error",
                    msg: "Invalid or disconnected node"
                };
            }
            node_status_reduced.push(node);
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
        nodes_status: node_status_reduced,
        elapsed_time: new Date() - Date.parse(trade.timeUTC)
    });

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
        context.callbackWaitsForEmptyEventLoop = false;
        
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