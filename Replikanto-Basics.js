"use strict";
// Import the dependency.
const fetchTimeout = require('fetch-timeout');
var crypto = require('crypto');

const DynamoDB = require('aws-sdk/clients/dynamodb');
const ApiGatewayManagementApi = require('aws-sdk/clients/apigatewaymanagementapi');
const SNS = require('aws-sdk/clients/sns');
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

const ConnectionTableName = process.env.DYNAMODB_TABLE_CONNECTION;
const MachineIDTableName = process.env.DYNAMODB_TABLE_MACHINE_ID;
const ActiveMachineIDTableName = process.env.DYNAMODB_TABLE_ACTIVE_MACHINE_ID;
const BroadcastTableName = process.env.DYNAMODB_TABLE_BROADCAST;
const BroadcastPositionInfoTableName = process.env.DYNAMODB_TABLE_BROADCAST_POSITION_INFO;
const MD5Key = process.env.MD5Key;
const MachineIDsBlackList = process.env.MACHINE_IDS_BLACK_LIST.split(',');
const VendorName = process.env.VENDOR_NAME;
const VendorPassword = process.env.VENDOR_PWD;
const ActiveNotifyInterval = process.env.ACTIVE_NOTIFY_INTERVAL;
const TopicAdmMsg = process.env.TOPIC_ADMIN_MESSAGES;
const TopicAdmChatID = process.env.TOPIC_ADMIN_CHAT_ID;
const TopicAdmToken = process.env.TOPIC_ADMIN_TOKEN;

const re_machine_id = /[0-9A-Fa-f]{32}(-[A-Za-z0-9]{1,60}|)/;
const re_signed_machine_id = /[0-9A-Fa-f]{32}(-[A-Za-z0-9]{1,60})/;

function uniqueReplikantoID() {
  function chr4() {
    return Math.random().toString(36).slice(-4).toUpperCase();
  }
  
  return `@REP-${chr4()}-${chr4()}`;
}

async function newReplikantoID(machine_id, credits, db, time, bkp = undefined) {
    let unique_replikanto_id;
    let count = 0;
    let times = 0;
    do {
        unique_replikanto_id = uniqueReplikantoID();
        //console.log(unique_replikanto_id);
        if (unique_replikanto_id === "@REP-XXXX-XXXX" || unique_replikanto_id === "@REP-TEST-ECHO") {
            times = times + 1;
            continue;
        }
        var params2 = {
            TableName: MachineIDTableName,
            IndexName: "ReplikantoID",
            ProjectionExpression: "replikanto_id",
            KeyConditionExpression: "replikanto_id = :val",
            ExpressionAttributeValues: {
                ":val": unique_replikanto_id
            }
        };
        
        const data2 = await db.query(params2).promise();
        count = data2.Count;
        //console.log("data2.Count", count);
        times = times + 1;
    } while (count > 0 && times < 5);

    const last_update = time instanceof Date ? time.getTime() : time;
    let ret = await db
        .put({
            TableName: MachineIDTableName,
            Item: {
                machine_id : machine_id,
                replikanto_id: unique_replikanto_id,
                credits: parseInt(credits, 10),
                last_update,
                unassigned_machine_id : CleanMachineID(machine_id),
                bkp
            }
        })
        .promise();
    
    //console.log(ret);
    //console.log(unique_replikanto_id);
    
    return unique_replikanto_id;
}

function CleanMachineID(machine_id) {
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

function CleanSignedMachineID(machine_id) {
    if (machine_id === undefined) {
        throw 'Undefined machine id';
    }
    
    if (machine_id.length < 32) {
        throw `Invalid ${machine_id} machine id`;
    }

    let m;
    if ((m = re_signed_machine_id.exec(machine_id)) !== null) {
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

async function sendToConnection(requestContext, connection_id, local_region, data) { // parece que é mais lento do que a função antiga
    //let endpoint = requestContext.domainName + '/' + requestContext.stage;
    if (local_region === undefined) {
        local_region = region;
    }
    const wsApiId = process.env["WS_API_ID_" + local_region.toUpperCase().replace(/-/g, "_")];
    let stage = requestContext.stage;
    if (stage === "test-invoke-stage") {
        stage = "dev";
    }
    let endpoint = `https://${wsApiId}.execute-api.${local_region}.amazonaws.com/${stage}`;

    const callbackAPI = new ApiGatewayManagementApi({
        apiVersion: '2018-11-29',
        endpoint: endpoint,
        region: local_region
    });

    try {
        await callbackAPI
            .postToConnection({ ConnectionId: connection_id, Data: JSON.stringify(data) })
            .promise();
        return { status: 200 };
    } catch (e) {
        console.error(connection_id, e.code, e.statusCode);
        await SNSPublish("Send to Connetion", `${connection_id}, ${e.code}, ${e.statusCode}`);
        return {
            status: e.statusCode,
            statusText: e.code
        };
    }
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

async function MachineIdRelation(db, machine_id, projectionExpression = "replikanto_id") {
    var params = {
        TableName: MachineIDTableName,
        ProjectionExpression: projectionExpression,
        KeyConditionExpression: "machine_id = :val",
        ExpressionAttributeValues: {
            ":val": machine_id
        }
    };
    
    const data = await db.query(params).promise();
    if (data.Count > 0) {
        return data;
    }
    return false;
}

async function UnassignedMachineIdRelation(db, unassigned_machine_id, projectionExpression = "machine_id") {
    var params = {
        TableName: MachineIDTableName,
        IndexName: "UnassignedMachineID",
        ProjectionExpression: projectionExpression,
        KeyConditionExpression: "unassigned_machine_id = :val",
        ExpressionAttributeValues: {
            ":val": unassigned_machine_id
        }
    };
    
    const data = await db.query(params).promise();
    if (data.Count > 0) {
        return data;
    }
    return false;
}

async function UpdateDataBase(db, assigned_machine_id_obj) {
    let data = await MachineIdRelation(db, assigned_machine_id_obj.AssignedMachineId, "replikanto_id, credits, bkp");
    if (data === false) {
        data = await MachineIdRelation(db, assigned_machine_id_obj.MachineId, "replikanto_id, credits, bkp");
        if (data !== false) {
            // Tem dados antigos, então vamos atualizar...
            try {
                await db.transactWrite({
                    TransactItems: [
                        {
                            Put: {
                                TableName: MachineIDTableName,
                                Item: {
                                    machine_id: assigned_machine_id_obj.AssignedMachineId,
                                    replikanto_id: data.Items[0].replikanto_id,
                                    credits: data.Items[0].credits,
                                    last_update: new Date().getTime(),
                                    unassigned_machine_id: assigned_machine_id_obj.MachineId,
                                    bkp: data.Items[0].bkp
                                },
                                //ConditionExpression: "machine_id <> :f",
                                //ExpressionAttributeValues: {
                                    //":f": machine_id
                                //}
                            }
                        },
                        {
                            Delete: {
                                TableName: MachineIDTableName,
                                Key: {
                                    machine_id : assigned_machine_id_obj.MachineId
                                }
                            }
                        }
                    ]
                }).promise();
                return true;
            } catch (error) {
                throw error;
            }
        }
    }
    return false;
}

async function CheckConnectedMachineID(db, assigned_machined_id_obj) {
    const machined_id = assigned_machined_id_obj.MachineId;
    const assigned_machined_id = assigned_machined_id_obj.AssignedMachineId;
    
    let machined_id_connected = machined_id;
    
    const data = await db.query({
        TableName: ConnectionTableName,
        IndexName: "MachineID",
        ProjectionExpression: "machine_id",
        KeyConditionExpression: "machine_id = :val",
        ExpressionAttributeValues: {
            ":val": assigned_machined_id
        },
    }).promise();
    
    if (data.Count > 0) {
        machined_id_connected = data.Items[0].machine_id;
    }
    
    return machined_id_connected;
}

async function LicenseType(machine_id, product_name, isProd) {
    if (!isProd) {
        return "DEBUG";
    }
    let unassignedMacineId = machine_id;
    const isAssignedMachineId = machine_id.includes('-', 32);
    if (isAssignedMachineId) {
        unassignedMacineId = machine_id.substr(0, 32);
    }
    const opts = { 
        method: 'GET',
        host: "license.ninjatrader.com",
    };
    
    if (product_name === undefined) {
        product_name = "Replikanto";
    }
    
    // TODO filtrar para que a data final seja maior que hoje, está vindo machine ids vencidos.
    const urlRegular = `http://${opts.host}/tools/NtVendorLicense.php?ac=al&vd=${VendorName}&pw=${VendorPassword}&md=${product_name}&mc=${machine_id}`;
    const urlTrial   = `http://${opts.host}/tools/NtVendorLicense.php?ac=af&vd=${VendorName}&pw=${VendorPassword}&md=${product_name}&mc=${unassignedMacineId}`;

    let license_type;
    
    // TODO colocar os dois fetch em paralelo, quando possível, pois durante a conexão não importa muito a velocidade.
    try {
        let response = await fetchTimeout(urlRegular, opts, 500, "Ninjatrader Server Timeout");
        
        if (response.status === 200) {
            let body = await response.text();
            if (body.indexOf("<LicenseType>") > 0) {
                license_type = body.substring(body.indexOf("<LicenseType>") + 13, body.indexOf("</LicenseType>"));
            }
        }
        
        if (license_type !== undefined) {
            return license_type;
        }
    
        response = await fetchTimeout(urlTrial, opts, 500, "Ninjatrader Server Timeout");
    
        if (response.status === 200) {
            let body = await response.text();
            if (body.indexOf("<LicenseType>") > 0) {
                license_type = body.substring(body.indexOf("<LicenseType>") + 13, body.indexOf("</LicenseType>"));
            }
        }
        
        if (license_type !== undefined) {
            return license_type;
        }
    } catch (error) {
        console.log(`License Type Error for machine id ${machine_id}`);
        throw error;
    }
    
    throw `License Type Undefined for machine id ${machine_id}`;
}

async function BroadcastList(db, machine_id) {
    try {
        //console.log("BroadcastList", machine_id);
        const data = await db.scan({
            TableName: BroadcastTableName,
            ProjectionExpression: "broadcast_list_id, broadcast_id, broadcast_name",
            FilterExpression: "contains (followers_machine_ids, :val)",
            ExpressionAttributeValues: {
                ":val": machine_id
            },
        }).promise();
        //console.log("BroadcastList", data);
        let list = [];
        
        data.Items.forEach(function(element, index, array) {
            //console.log(element);
            list.push({
                broadcast_list_id: element.broadcast_list_id,
                id: element.broadcast_id,
                name: element.broadcast_name
            });
        });

        return list;
    } catch (error) {
        console.error(error);
        return [];
    }
}

async function BroadcastChangeMachineID(db, broadcast_list_id, machine_id, action, type) {
    const machine_ids = db.createSet([machine_id]);
    await db.update({
        TableName: BroadcastTableName,
        Key: { broadcast_list_id },
        ExpressionAttributeValues: { ":var1": machine_ids },
        UpdateExpression: `${action} ${type}_machine_ids :var1`,
        ReturnValues: "NONE"
    }).promise();
    console.log(action, type, machine_id, "for broadcast list id", broadcast_list_id);
}

async function BroadcastChangeMachineIDInfo(db, old_machined_id, new_machine_id, delete_old_machine_id = false) {
    // Find the broadcast list follower/owner old machine ID
    let broadcast_list_id_ret = await db.scan({
        TableName: BroadcastTableName,
        ProjectionExpression: "broadcast_list_id, followers_machine_ids, owner_machine_ids",
        ExpressionAttributeValues: { ":var1": old_machined_id },
        FilterExpression: "contains(followers_machine_ids, :var1) OR contains(owner_machine_ids, :var1)",
        ReturnValues: "NONE"
    }).promise();
    let did = false;
    for (let i = 0 ; i < broadcast_list_id_ret.Count; i++) {
        // Change the broadcast list follower/owner old machine ID to a new Machine ID
        let broadcast_list_data = broadcast_list_id_ret.Items[i];
        if (broadcast_list_data.followers_machine_ids.values.find(e => e === old_machined_id) != undefined) {
            if (broadcast_list_data.followers_machine_ids.values.find(e => e === new_machine_id) == undefined) {
                await BroadcastChangeMachineID(db, broadcast_list_data.broadcast_list_id, new_machine_id, "add", "followers");
                did = true;
            }
            if (delete_old_machine_id) {
                await BroadcastChangeMachineID(db, broadcast_list_data.broadcast_list_id, old_machined_id, "delete", "followers");
                did = true;
            }
        }
        if (broadcast_list_data.owner_machine_ids.values.find(e => e === old_machined_id) != undefined) {
            if (broadcast_list_data.owner_machine_ids.values.find(e => e === new_machine_id) == undefined) {
                await BroadcastChangeMachineID(db, broadcast_list_data.broadcast_list_id, new_machine_id, "add", "owner");
                did = true;
            }
            if (delete_old_machine_id) {
                await BroadcastChangeMachineID(db, broadcast_list_data.broadcast_list_id, old_machined_id, "delete", "owner");
                did = true;
            }
        }
    }
    return did;
}

async function BroadcastListIDFromOwner(db, broadcast_list_id, machine_id) {
    try {
        const data = await db.query({
            TableName: BroadcastTableName,
            KeyConditionExpression: "broadcast_list_id = :val1",
            ProjectionExpression: "broadcast_list_id",
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

        return data.Items[0]["broadcast_list_id"];
    } catch (error) {
        console.error(error);
        await SNSPublish("BroadcastListID", `${error}`);
        return undefined;
    }
}

async function BroadcastUpdatePositionInfo(db, broadcast_list_id, position, account) {
    //const market_position       = position.market_position;
    //const quantity              = position.quantity;
    //const average_price         = position.average_price;
    const { instrument, ...position_to_save} = position;

    //const account_name          = account.account_name;
    //const provider              = account.provider;

    await db.update({
        TableName: BroadcastPositionInfoTableName,
        Key: { broadcast_list_id },
        ExpressionAttributeNames: { '#instrument': instrument },
        ExpressionAttributeValues: { ":var1": { ...position_to_save, ...account, last_update: new Date().getTime() } },
        UpdateExpression: `SET #instrument = :var1`,
        ReturnValues: "NONE"
    }).promise();
}

async function BroadcastGetPositionInfo(db, broadcast_list_id) {
    console.log("BroadcastGetPositionInfo", broadcast_list_id);
    try {
        const data = await db.query({
            TableName: BroadcastPositionInfoTableName,
            KeyConditionExpression: "broadcast_list_id = :val1",
            ExpressionAttributeValues: {
                ":val1": broadcast_list_id
            },
        }).promise();
        let list = [];
        data.Items.forEach(function(element) {
            list.push(element);
        });
        return list;
    } catch (error) {
        console.error(error);
        return [];
    }
}

async function ReplaceMachineID(db, old_machine_id, old_replikanto_id, old_credits, machine_id, unsigned_machine_id, bkp, connectedAt) {
    const last_update = connectedAt instanceof Date ? connectedAt.getTime() : connectedAt;
    
    await db.transactWrite({
        TransactItems: [
            {
                Put: {
                    TableName: MachineIDTableName,
                    Item: {
                        machine_id: machine_id,
                        replikanto_id: old_replikanto_id,
                        credits: old_credits,
                        last_update: last_update,
                        unassigned_machine_id: unsigned_machine_id,
                        bkp
                    },
                }
            },
            {
                Delete: { // Apagar a informação antiga
                    TableName: MachineIDTableName,
                    Key: {
                        machine_id : old_machine_id
                    }
                }
            }
        ]
    }).promise();
}

async function SNSPublish(subject, msg) {
    await new SNS().publish({
        Subject: subject,
        Message: JSON.stringify({
            msg,
            chat_id: TopicAdmChatID,
            token: TopicAdmToken
        }),
        TopicArn: TopicAdmMsg,
    }).promise();
}

var functions = [];

functions.connect = async function(headers, paths, requestContext, body, db, isProd) {
    const connection_id             = requestContext.connectionId;
    const connectedAt               = requestContext.connectedAt;
    const replikanto_version        = headers["Replikanto-Version"];
    const product_name              = headers["Product-Name"];
    //let unsigned_machine_id         = undefined;
    let machine_id                  = undefined;
    
    try {
        const signed_machine_id_obj = CleanSignedMachineID(headers["Machine-Id"]);
        machine_id                  = signed_machine_id_obj.AssignedMachineId;
    } catch (error) {
        const msgInfo = `Unsigned Machine ID ${headers["Machine-Id"]} for product ${product_name} version ${replikanto_version}`;
        console.warn(msgInfo);
        await SNSPublish("Connect", msgInfo);
        /*
        try {
            unsigned_machine_id     = CleanMachineID(headers["Machine-Id"]);
            machine_id              = unsigned_machine_id;
        } catch (error) {
            console.error("" + error);
            return false;
        }
        */
        return true;
    }

    if (MachineIDsBlackList.includes(machine_id)) { // Tem tbm um trecho verificando o blacklist no Flow
        const msgInfo = `Machine ID ${machine_id} is in the Black List`;
        console.warn(msgInfo);
        await SNSPublish("Connect", msgInfo);
        return false;
    }
    
    let license_type                = undefined;
    /*
    try {
        license_type = await LicenseType(machine_id, product_name, isProd);
        console.log(`License Type ${license_type} for Machine ID ${machine_id} product ${product_name} version ${replikanto_version}`);
    } catch (error) {
        console.error("" + error);
        await SNSPublish("Connect", `${error}`);
    }
    */

    let replikanto_id = "CONTACT-SUPPORT";

    const data = await MachineIdRelation(db, machine_id, "replikanto_id");
    if (data === false) {
        let old_machine_id      = undefined;
        if ("Old-Machine-Id" in headers) {
            old_machine_id      = headers["Old-Machine-Id"];
            console.log("Old machine id", old_machine_id, "is connecting with a new machine id", machine_id);
            //const old_data = await MachineIdRelation(db, old_machine_id, "replikanto_id, credits, last_update, unassigned_machine_id, bkp");
            //if (old_data === false) {
                // Aqui então podemos criar um novo Rep Id pois não tem old machine id
                //replikanto_id = await newReplikantoID(machine_id, process.env.INITIAL_CREDITS, db, connectedAt);
            //} else {
                // Find the broadcast list follower/owner old machine ID and then change it.
                //await BroadcastChangeMachineIDInfo(db, old_machine_id, machine_id, false);
                /* Não vamos fazer a mudança e sim informar para que o usuário faça a mudança do machine id dele...
                const old_replikanto_id         = old_data.Items[0].replikanto_id;
                const old_credits               = old_data.Items[0].credits;
                const old_bkp                   = old_data.Items[0].bkp;
                const old_last_update           = old_data.Items[0].last_update;
                const old_unassigned_machine_id = old_data.Items[0].unassigned_machine_id;

                // Vamos fazer bkp da informação antiga                
                let bkp = [{
                    machine_id: old_machine_id,
                    replikanto_id: old_replikanto_id,
                    credits: old_credits,
                    last_update: old_last_update,
                    unassigned_machine_id: old_unassigned_machine_id,
                    bkp: old_bkp
                }];
                
                console.log("Old-Replikanto-Id", old_replikanto_id);

                await ReplaceMachineID(db, old_machine_id, old_replikanto_id, old_credits, machine_id, unsigned_machine_id, bkp, connectedAt);

                replikanto_id = old_replikanto_id;
                */
            //}
        } else {
            // Aqui então podemos criar um novo Rep Id pois não tem old machine id
            //replikanto_id = await newReplikantoID(machine_id, process.env.INITIAL_CREDITS, db, connectedAt);
        }
        replikanto_id = await newReplikantoID(machine_id, process.env.INITIAL_CREDITS, db, connectedAt);
    } else {
        replikanto_id = data.Items[0].replikanto_id;
    }

    await db
        .put({
            TableName: ConnectionTableName,
            Item: {
                replikanto_id,
                machine_id,
                connection_id,
                createdDate: connectedAt,
                replikanto_version,
                //license_type,
                region
            }
        })
        .promise();
    
    console.log(`Connected id ${connection_id} for Machine ID ${machine_id} product ${product_name} version ${replikanto_version} and region ${region}`);

    // TODO se tiver algum trade pendente para receber, que seja agora... Será que dá?
    
    return true;
};

functions.disconnect = async function(headers, paths, requestContext, body, db, isProd) {
    const connection_id = requestContext.connectionId;

    await db
        .delete({
            TableName: ConnectionTableName,
            Key: {
                connection_id,
            }
        })
        .promise();
    
    console.log(`Disconnected connection id ${connection_id}`);
    
    return true;
};

functions.nodeinfo = async function(headers, paths, requestContext, body, db, isProd) {
    //const connection_id = requestContext.connectionId;
    //console.log(headers);
    //console.log(body);

    const replikanto_version        = body.replikanto_version;
    let machine_id                  = undefined;
    try {
        const signed_machine_id_obj = CleanSignedMachineID(body.machine_id);
        machine_id                  = signed_machine_id_obj.AssignedMachineId;
        //if (await UpdateDataBase(db, assigned_machine_id)) {
            //console.log(`Updated machine id ${assigned_machine_id.MachineId} to ${assigned_machine_id.AssignedMachineId}`);
        //}
    } catch (error) {
        const msgInfo = `Unsigned Machine ID ${body.machine_id} for version ${replikanto_version}, Replikanto ID = CONTACT-SUPPORT`;
        console.warn(msgInfo);
        await SNSPublish("Node Info", msgInfo);
        //const assigned_machine_id_data = await UnassignedMachineIdRelation(db, machine_id);
        //if (assigned_machine_id_data !== false && assigned_machine_id_data.Items[0].machine_id !== machine_id) {
            return {
                action: "node_info",
                payload: {
                    replikanto_id: "CONTACT-SUPPORT",
                    credits: 0
                }
            };
        //}
    }

    //const replikanto_version = body.replikanto_version;
    const data = await MachineIdRelation(db, machine_id, "replikanto_id, credits");
    
    let credits = 0;
    let replikanto_id = "";

    if (data === false) {
        let old_machine_id = undefined;
        if ("Old-Machine-Id" in headers) { // headers só vem quando é chamada via http, então quando for ws eu já pego o old no momento da conexão... mas aqui se eu pego se o cliente não estiver usando o ws.
            old_machine_id = headers["Old-Machine-Id"];
            console.log("Old machine id", old_machine_id, "is getting node info with a new machine id", machine_id);
            //const old_data = await MachineIdRelation(db, old_machine_id, "replikanto_id, credits, last_update, unassigned_machine_id, bkp");
            //if (old_data === false) {
                // Aqui então podemos criar um novo Rep Id pois não tem old machine id
                //credits = process.env.INITIAL_CREDITS;
                //replikanto_id = await newReplikantoID(machine_id, credits, db, new Date());
            //} else {
                // Find the broadcast list follower/owner old machine ID and then change it.
                //await BroadcastChangeMachineIDInfo(db, old_machine_id, machine_id, false);
                /* 
                const old_replikanto_id         = old_data.Items[0].replikanto_id;
                const old_credits               = old_data.Items[0].credits;
                const old_bkp                   = old_data.Items[0].bkp;
                const old_last_update           = old_data.Items[0].last_update;
                const old_unassigned_machine_id = old_data.Items[0].unassigned_machine_id;

                // Vamos fazer bkp da informação antiga                
                let bkp = [{
                    machine_id: old_machine_id,
                    replikanto_id: old_replikanto_id,
                    credits: old_credits,
                    last_update: old_last_update,
                    unassigned_machine_id: old_unassigned_machine_id,
                    bkp: old_bkp
                }];
                
                console.log("Old-Replikanto-Id", old_replikanto_id);

                await ReplaceMachineID(db, old_machine_id, old_replikanto_id, old_credits, machine_id, unsigned_machine_id, bkp, new Date());
                
                credits = old_credits;
                replikanto_id = old_replikanto_id;
                */
            //}
        } else {
            //credits = process.env.INITIAL_CREDITS;
            //replikanto_id = await newReplikantoID(machine_id, credits, db, new Date());
        }
        credits = process.env.INITIAL_CREDITS;
        replikanto_id = await newReplikantoID(machine_id, credits, db, new Date());
    } else {
        credits = data.Items[0].credits;
        replikanto_id = data.Items[0].replikanto_id;
    }

    // The same code as above is listed in change_machine_id method, if you change it here, please change it there
    let broadcast_list = await BroadcastList(db, machine_id);

    for (var i = 0; i < broadcast_list.length; i++) {
        const broadcast_list_id = broadcast_list[i].broadcast_list_id;
        delete broadcast_list[i].broadcast_list_id;

        const positions = await BroadcastGetPositionInfo(db, broadcast_list_id);
        if (positions.length > 0) {
            broadcast_list[i]["positions"] = positions[0];
            delete broadcast_list[i].positions.broadcast_list_id;
        }
    }

    return {
        action: "node_info",
        payload: {
            replikanto_id,
            credits,
            broadcast_list
        }
    };
};

functions.positioninfo = async function(headers, paths, requestContext, body, db, isProd) {
    console.log(body);
    
    const machine_id            = body.machine_id;
    
    const nodes                 = [...new Set(body.nodes)].filter(function (e) { return e != null; });
    const broadcast_lists       = nodes.filter(function(n) { return n.startsWith("@LST") });

    if (broadcast_lists !== undefined && broadcast_lists.length > 0) {
        await Promise.all(broadcast_lists.map(async (broadcast_list_id_local) => {
            const broadcast_list_id = await BroadcastListIDFromOwner(db, broadcast_list_id_local, machine_id);
            if (broadcast_list_id != undefined) {
                // aqui sim podemos guardar a info
                await BroadcastUpdatePositionInfo(db, broadcast_list_id, body.position, body.account);
            }
        }));
    }

    return true;
}

/**
 * PUT machine ID from FlowBots WordPress API
 **/
functions.change_machine_id = async function(headers, paths, requestContext, body, db, isProd) {
    let old_unsigned_machine_id = undefined, unsigned_machine_id = undefined;
    try {
        old_unsigned_machine_id = CleanMachineID(paths.machine_id);
        unsigned_machine_id = CleanMachineID(body.machine_id);

        console.log(`Old unsigned machine id ${old_unsigned_machine_id}`);
        console.log(`New unsigned machine id ${unsigned_machine_id}`);
    } catch (error) {
        // the old machine id cannot be invalid
        return {
            action: "change_machine_id",
            payload: {
                status: "error",
                msg: error
            }
        };
    }

    let old_signed_machine_id = undefined, signed_machine_id = undefined;
    try {
        old_signed_machine_id = CleanSignedMachineID(paths.machine_id);
        console.log("Old signed machine id", old_signed_machine_id);
    } catch (error) {
        // the old machine id can be unsigned
    }
    try {
        signed_machine_id = CleanSignedMachineID(body.machine_id);
        console.log("New signed machine id", signed_machine_id);
    } catch (error) {
        // the new machine id cannot be unsigned
        return {
            action: "change_machine_id",
            payload: {
                status: "error",
                msg: error
            }
        };
    }
    
    const old_machine_id_to_check = old_signed_machine_id === undefined ? old_unsigned_machine_id : old_signed_machine_id.AssignedMachineId;
    const machine_id_to_check = signed_machine_id.AssignedMachineId;
    
    if (old_machine_id_to_check === machine_id_to_check) {
        return {
            action: "change_machine_id",
            payload: {
                status: "error",
                msg: "It is not possible to modify two identical Machine IDs"
            }
        };
    }

    try {
        // Find the broadcast list follower/owner old machine ID and then change it.
        if (await BroadcastChangeMachineIDInfo(db, old_machine_id_to_check, machine_id_to_check, true)) {
            console.info("Added " + machine_id_to_check + " to the broadcast list");
            console.info("Removed " + old_machine_id_to_check + " to the broadcast list");
            await SNSPublish("Change Machine ID", "Added " + machine_id_to_check + " to the broadcast list");
            await SNSPublish("Change Machine ID", "Removed " + old_machine_id_to_check + " to the broadcast list");
        }
    } catch (error) {
        console.error("" + error);
        await SNSPublish("Change Machine ID", `${error}`);
    }

    try {
        var params_to_find_old_machine_id = {
            TableName: MachineIDTableName,
            ProjectionExpression: "replikanto_id, credits, bkp, last_update",
            KeyConditionExpression: "machine_id = :val",
            ExpressionAttributeValues: {
                ":val": old_machine_id_to_check
            }
        };
        
        let data_old_machine_id = await db.query(params_to_find_old_machine_id).promise();
        
        let msg = "";
        let status = "changed";
        if (data_old_machine_id.Count == 1) {
            // found the old machine id relation
            const old_replikanto_id = data_old_machine_id.Items[0].replikanto_id;
            const old_credits = data_old_machine_id.Items[0].credits;
            let old_backup = data_old_machine_id.Items[0].bkp;
            const old_last_update = data_old_machine_id.Items[0].last_update;

            const new_bkp = {
                machine_id: old_machine_id_to_check,
                replikanto_id: old_replikanto_id,
                credits: old_credits,
                last_update: old_last_update
            };
            
            var params_to_find_new_machine_id = {
                TableName: MachineIDTableName,
                ProjectionExpression: "replikanto_id, credits, bkp, last_update",
                KeyConditionExpression: "machine_id = :val",
                ExpressionAttributeValues: {
                    ":val": machine_id_to_check
                }
            };
            
            let data_new_machine_id = await db.query(params_to_find_new_machine_id).promise();
            
            if (data_new_machine_id.Count == 1) {
                const new_replikanto_id = data_new_machine_id.Items[0].replikanto_id;
                const new_credits = data_new_machine_id.Items[0].credits;
                const new_last_update = data_new_machine_id.Items[0].last_update;
                let new_backup = data_new_machine_id.Items[0].bkp;

                if (old_backup != undefined) {
                    new_bkp["bkp"] = old_backup;
                }
                if (new_backup == undefined) {
                    new_backup = [new_bkp];
                } else {
                    new_backup.push(new_bkp);
                }

                // Verificar se o usuário está conectado com aquela novo Machine ID
                var params_to_find_new_machine_id_connected = {
                    TableName: ConnectionTableName,
                    IndexName: "MachineID",
                    ProjectionExpression: "connection_id, #region_name, replikanto_id",
                    KeyConditionExpression: "machine_id = :val",
                    ExpressionAttributeValues: {
                        ":val": machine_id_to_check
                    },
                    ExpressionAttributeNames: {
                        "#region_name": "region"
                    }
                };
                let restart_msg = undefined;
                let data_new_machine_id_connected = await db.query(params_to_find_new_machine_id_connected).promise();
                if (data_new_machine_id_connected.Count == 1) {
                    // Se estiver conectado vamos mudar o @REP id mesmo assim, e será guardado no bkp o recém criado
                    const actual_replikanto_id = data_new_machine_id_connected.Items[0].replikanto_id;
                    const connection_id = data_new_machine_id_connected.Items[0].connection_id;
                    const region = data_new_machine_id_connected.Items[0].region;

                    const msgInfo = `The new machine id ${machine_id_to_check} is connected without changing the machine id, so the newly created Replikanto ID ${actual_replikanto_id} will be replaced by the old good one ${old_replikanto_id} in both tables`;
                    console.warn(msgInfo);
                    await SNSPublish("Change Machine ID", msgInfo);

                    await db.update({
                        TableName: ConnectionTableName,
                        Key: { connection_id },
                        UpdateExpression: "set replikanto_id = :val1",
                        ExpressionAttributeValues: { ":val1": old_replikanto_id },
                        ReturnValues: "NONE"
                    }).promise();

                    try {
                        // nodeinfo
                        let broadcast_list = await BroadcastList(db, machine_id_to_check);
                        await sendToConnection(requestContext, connection_id, region, {
                            action: "node_info",
                            payload: {
                                replikanto_id : old_replikanto_id,
                                credits: old_credits,
                                broadcast_list
                            }
                        });
                    } catch (e) {
                        console.error(e);
                        await SNSPublish("Change Machine ID", `${e}`);
                        restart_msg = "Please restart Ninjatrader";
                    }
                }
                
                new_backup.push({
                    machine_id: machine_id_to_check,
                    replikanto_id: new_replikanto_id,
                    credits: new_credits,
                    last_update: new_last_update,
                    msg
                });

                await db.update({
                    TableName: MachineIDTableName,
                    Key: { machine_id: machine_id_to_check },
                    ExpressionAttributeValues: { ":val1": new_backup, ":val2": old_replikanto_id, ":val3": old_credits, ":val4": new Date().getTime(), ":val5": signed_machine_id.MachineId },
                    UpdateExpression: "set bkp = :val1, replikanto_id = :val2, credits = :val3, last_update = :val4, unassigned_machine_id = :val5",
                    ReturnValues: "NONE"
                }).promise();

                console.info(`Machine ID change was successful. The old good Replikanto ID ${old_replikanto_id} was kept from the old machined id ${old_machine_id_to_check}, the newly created Replikanto ID ${new_replikanto_id} is saved in bkp attibute`);

                msg = `Machine ID change was successful.\nYour Replikanto ID is ${old_replikanto_id}.`;
                if (restart_msg !== undefined) {
                    msg = msg + "\n" + restart_msg;
                }
            } else {
                if (old_backup == undefined) {
                    old_backup = [new_bkp];
                } else {
                    old_backup.push(new_bkp);
                }
                
                // Aqui vai cadastrar o new com as informações do old
                await db.put({
                    TableName: MachineIDTableName,
                    Item: { 
                        machine_id: machine_id_to_check, 
                        credits: old_credits, 
                        last_update: new Date().getTime(), 
                        replikanto_id: old_replikanto_id, 
                        unassigned_machine_id: signed_machine_id.MachineId,
                        bkp: old_backup
                    }
                }).promise();
                

                msg = `Machine ID change was successful.\nYour Replikanto ID is ${old_replikanto_id}.`;
            }
            // Apagar o old que não serve para mais nada
            await db.delete({
                TableName: MachineIDTableName,
                Key: {
                    machine_id : old_machine_id_to_check
                }
            }).promise();
        } else {
            status = "error";
            msg = `Machine ID ${old_machine_id_to_check} not found in Replikanto database`;
        }

        await SNSPublish("Change Machine ID", msg);

        return {
            action: "change_machine_id",
            payload: {
                status: status,
                machine_id: old_machine_id_to_check,
                new_machine_id: machine_id_to_check,
                msg
            }
        };
    } catch (error) {
        console.error(error);
        await SNSPublish("Change Machine ID", `${error}`);
        return {
            action: "change_machine_id",
            payload: {
                status: "error",
                msg: "" + error
            }
        };
    }
    
};

/**
 * PUT machine ID from Replikanto C# to check for duplicate instances (VM clone issue)
 **/
functions.active_machine_id = async function(headers, paths, requestContext, body, db, isProd) {
    const interval = parseInt(ActiveNotifyInterval, 10);
    const ttl = Math.floor((new Date().getTime() + interval * 60000) / 1000);
    const ip = requestContext?.identity?.sourceIp;
    const seed = headers["guid"];

    let is_assigned_machine_id = false;
    let machine_id;
    try {
        machine_id = CleanSignedMachineID(headers["Machine-Id"]);
        machine_id = machine_id.AssignedMachineId;
        is_assigned_machine_id = true;
    } catch (error) {
        machine_id = CleanMachineID(headers["Machine-Id"]);
    }
    //console.log(headers);
    //console.log(requestContext);
    //console.log(body);

    let status = "active";
    let msg = "";

    try {
        // Verificar quantos online tem
        const ttl_now = Math.floor(new Date().getTime() / 1000);
        const data = await db.query({
                TableName: ActiveMachineIDTableName,
                IndexName: "MachineID",
                ProjectionExpression: "id, ip, #ttl_name",
                KeyConditionExpression: "machine_id = :val1",
                FilterExpression: "#ttl_name > :val2 AND ip <> :val3",
                ExpressionAttributeValues: {
                    ":val1": machine_id,
                    ":val2": ttl_now,
                    ":val3": ip
                },
                ExpressionAttributeNames: {
                    "#ttl_name": "ttl"
                }
            }).promise();
        if (data.Count > 0) {
            const msgInfo = `Unable to activate machine id ${machine_id}. There are ${data.Count} instance(s) already opened.`;
            console.warn(msgInfo);
            await SNSPublish("Active Machine ID", msgInfo);
            
            if (is_assigned_machine_id) { // target only assigned machine ids
                status = "invalid";
                //msg = "You already have another Replikanto running with the same machine id. To use Replikanto, close all Ninjatraders with the same machine id and reopen this one. You can ask for help by email {0}.";
                msg = "This product is licensed for use on a single computer only, another Replikanto is already activated with this machine id. To be able to use it, close all others. You can ask for help by email {0}.";
            } else {
                // esse aqui em baixo depois será removido e as linhas acima descomentadas.
                await db
                    .put({
                        TableName: ActiveMachineIDTableName,
                        Item: {
                            id: seed,
                            machine_id,
                            ttl,
                            ip,
                            has_duplicates: true
                        }
                    })
                    .promise();
            }
        } else {
            console.log(`Activate machine id ${machine_id}`);
            // atualizar com mais um
            await db
                .put({
                    TableName: ActiveMachineIDTableName,
                    Item: {
                        id: seed,
                        machine_id,
                        ttl,
                        ip
                    }
                })
                .promise();
        }
    } catch (error) {
        console.error("Unable to update the active machine id table");
        console.error("Error", "" + error);
        await SNSPublish("Active Machine ID", "" + error);
    }

    const nakedStr = machine_id + ":" + seed + ":" + status + ":" + interval;

    const payload = {
        status: status,
        hash: crypto.createHmac("sha256", MD5Key).update(nakedStr).digest("hex"),
        msg,
        interval
    };

    return {
        action: "active_machine_id",
        payload
    };
};

/**
 * DELETE machine ID from Replikanto C# to release machine id instance
 **/
functions.disabled_machine_id = async function(headers, paths, requestContext, body, db, isProd) {

    let machine_id;
    try {
        machine_id = CleanSignedMachineID(headers["Machine-Id"]);
        machine_id = machine_id.AssignedMachineId;
    } catch (error) {
        machine_id = CleanMachineID(headers["Machine-Id"]);
    }
    
    const replikanto_version = headers["Replikanto-Version"];
    
    console.log("Deactive machine id " + machine_id);
    //console.log(requestContext);
    //console.log(body);

    const payload = {
        status: "disabled"
    };

    try {
        const data = await db.query({
                TableName: ActiveMachineIDTableName,
                IndexName: "MachineID",
                ProjectionExpression: "id, #ttl_name",
                KeyConditionExpression: "machine_id = :val1",
                ExpressionAttributeValues: {
                    ":val1": machine_id
                },
                ExpressionAttributeNames: {
                    "#ttl_name": "ttl"
                }
            }).promise();
        if (data.Count > 0) {
            const sort_itens = data.Items.sort(function(a, b) {
                let ttl_a = a.ttl, ttl_b = b.ttl;
                return ((ttl_a < ttl_b) ? -1 : ((ttl_a > ttl_b) ? 1 : 0));
            });
            
            const hash = headers["hash"];
            if (hash !== undefined) {
                for (let i = data.Count -1; i >= 0; i--) {
                    const last_item = sort_itens[i];
                    const nakedStr = `${machine_id}:${last_item.id}`;
                    const toCheck = crypto.createHmac("sha256", MD5Key).update(nakedStr).digest("hex");
                    if (toCheck === hash) {
                        await db.delete({
                                TableName: ActiveMachineIDTableName,
                                Key: { id : last_item.id }
                            }).promise();
                        break;
                    }
                }
            } else {
                if (compareVersion(replikanto_version, 1, 4, 1, 1) < 0) {
                    const last_item = sort_itens[data.Count -1];
                    await db.delete({
                        TableName: ActiveMachineIDTableName,
                        Key: { id : last_item.id }
                    }).promise();
                } else {
                    // TODO aqui pode ser pelo o IP tbm, em último caso 
                     
                    // aqui estão querendo hackear
                    throw "Missing the hash";
                }
            }
        }

    } catch (error) {
        console.error("Unable to delete the active machine id table");
        console.error("Error", "" + error);
        await SNSPublish("Disable Machine ID", "" + error);
    }

    return {
        action: "disabled_machine_id",
        payload
    };
};

functions.credit = async function(headers, paths, requestContext, body, db, isProd) {
    const replikanto_id = decodeURIComponent(paths.replikanto_id);
    const credit = body.credit;
    if (credit === undefined) {
        return {
            action: "credit",
            payload: {
                status: "error",
                msg: "Invalid credit"
            }
        };
    }
    if (!Number.isInteger(credit)) {
        return {
            action: "credit",
            payload: {
                status: "error",
                msg: "Invalid credit quantity type"
            }
        };
    }
    if (credit < 1) { //|| credit > 3000) {
        return {
            action: "credit",
            payload: {
                status: "error",
                msg: "Invalid credit quantity"
            }
        };
    }

    let orderId = body.order_id; // quando vem do wordpress após confirmação de compra vem o order_id
    if (orderId === undefined) {
        orderId = 0;
    } else {
        if (!Number.isInteger(orderId)) {
            return {
                action: "credit",
                payload: {
                    status: "error",
                    msg: "Invalid order id type"
                }
            };
        }
    }
    
    var params = {
        TableName: MachineIDTableName,
        IndexName: "ReplikantoID",
        ProjectionExpression: "machine_id, last_order_id",
        KeyConditionExpression: "replikanto_id = :val",
        ExpressionAttributeValues: {
            ":val": replikanto_id
        }
    };
    const data = await db.query(params).promise();
    if (data.Count == 0) {
        return {
            action: "credit",
            payload: {
                status: "error",
                msg: `The Replikanto ID ${replikanto_id} has no machine id`
            }
        };
    }
    const machine_id = data.Items[0].machine_id;
    const last_order_id = data.Items[0].last_order_id;
    
    if (orderId === last_order_id && last_order_id !== 0) {
        return {
            action: "credit",
            payload: {
                status: "error",
                msg: "Credit Twice Prevent"
            }
        };
    }

    const ret = await db.update({
        TableName: MachineIDTableName,
        Key: { machine_id },
        ExpressionAttributeValues: { ":num": credit, ":datetime": new Date().getTime(), ":orderId": orderId },
        UpdateExpression: "set credits = credits + :num, last_update = :datetime, last_order_id = :orderId",
        ReturnValues: "UPDATED_NEW"
    }).promise();
    
    
    const dataConnection = await db.query({
        TableName: ConnectionTableName,
        IndexName: "ReplikantoID",
        ProjectionExpression: "connection_id, #region_name",
        KeyConditionExpression: "replikanto_id = :val",
        ExpressionAttributeValues: {
            ":val": replikanto_id
        },
        ExpressionAttributeNames: {
            "#region_name": "region"
        }
    }).promise();
    
    for (let i = 0; i < dataConnection.Count; i++) {
        const response = await sendToConnection(requestContext, dataConnection.Items[i].connection_id, dataConnection.Items[i].region, {
            action: "credit",
            payload: {
                status: "credited",
                replikanto_id,
                credits: ret.Attributes.credits,
                credit
            }
        });
        if (response.status !== 200) {
            console.warn(response);
        }
    }
    
    await SNSPublish("Credit", `Credited ${credit} on Replikanto ID ${replikanto_id} total ${ret.Attributes.credits} using order ${orderId}`);

    return {
        action: "credit",
        payload: {
            status: "credited",
            replikanto_id,
            credits: ret.Attributes.credits,
            credit
        }
    };
};

functions.broadcast_follower_link = async function(headers, paths, requestContext, body, db, isProd) {
    try {
        const machine_id = CleanSignedMachineID(body['machine_id']).AssignedMachineId;
        const broadcast_list_id = paths['broadcast_list_id'];
        
        await BroadcastChangeMachineID(db, broadcast_list_id, machine_id, "add", "followers");

        return {
            action: "broadcast_follower_link",
            payload: {
                status: "linked"
            }
        };
    } catch (error) {
        console.error("" + error);
        await SNSPublish("Broadcast Follower Link", `${error}`);
        return {
            action: "broadcast_follower_link",
            payload: {
                status: "error",
                msg: "" + error
            }
        };
    }
};

functions.broadcast_follower_unlink = async function(headers, paths, requestContext, body, db, isProd) {
    try {
        const machine_id = CleanSignedMachineID(body['machine_id']).AssignedMachineId;
        const broadcast_list_id = paths['broadcast_list_id'];
        console.log(machine_id, broadcast_list_id);
        await BroadcastChangeMachineID(db, broadcast_list_id, machine_id, "delete", "followers");

        return {
            action: "broadcast_follower_unlink",
            payload: {
                status: "unlinked"
            }
        };
    } catch (error) {
        console.error("" + error);
        await SNSPublish("Broadcast Follower Unlink", `${error}`);
        return {
            action: "broadcast_follower_unlink",
            payload: {
                status: "error",
                msg: "" + error
            }
        };
    }
};

functions.broadcast = async function(headers, paths, requestContext, body, db, isProd) {
    const broadcast_list_id = decodeURIComponent(paths['broadcast_list_id']);
    //console.log(broadcast_list_id);

    var params = {
        TableName: BroadcastTableName,
        ProjectionExpression: "followers_machine_ids, owner_machine_ids, broadcast_name, telegram_chat_id",
        KeyConditionExpression: "broadcast_list_id = :val",
        ExpressionAttributeValues: {
            ":val": broadcast_list_id
        }
    };
        
    let data = await db.query(params).promise();
    
    let followers = [], owners = [];
    let broadcast_name, telegram_chat_id;

    if (data.Count > 0) {
        followers = data.Items[0].followers_machine_ids;
        owners = data.Items[0].owner_machine_ids;
        broadcast_name = data.Items[0].broadcast_name;
        telegram_chat_id = data.Items[0].telegram_chat_id;
    }

    return {
        action: "broacast",
        payload: {
            broadcast_name,
            telegram_chat_id,
            status: "list",
            followers,
            owners
        }
    };
};

functions.default = async function(headers, paths, requestContext, body, db, isProd) {
    return {
        action: "default",
        payload: {
        }
    };
};

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
                    msg: "" + error
                }
            }),
            "isBase64Encoded": false
        };
    }
};