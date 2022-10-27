"use strict";
// Import the dependency.
const DynamoDB = require('aws-sdk/clients/dynamodb');
const Lambda = require('aws-sdk/clients/lambda');
const SNS = require('aws-sdk/clients/sns');

const region = process.env.AWS_REGION;
const lambda = new Lambda({
    apiVersion: "2015-03-31",
    endpoint: `lambda.${region}.amazonaws.com`
});

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
const BroadcastChuncks = process.env.BROADCAST_CHUNKS;
const TopicAdmMsg = process.env.TOPIC_ADMIN_MESSAGES;
const TopicAdmChatID = process.env.TOPIC_ADMIN_CHAT_ID;
const TopicAdmToken = process.env.TOPIC_ADMIN_TOKEN;
const slice_size = parseInt(BroadcastChuncks, 10);

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

async function FollowersConnectionBroadcastList(db, broadcast_list_id) {
    try {
        const data = await db.query({
            TableName: BroadcastTableName,
            KeyConditionExpression: "broadcast_list_id = :val1",
            ProjectionExpression: "connection_ids, broadcast_id, broadcast_name, telegram_chat_id, locale, time_zone",
            ExpressionAttributeValues: {
                ":val1": broadcast_list_id
            },
        }).promise();

        if (data.Count == 0) {
            return undefined;
        }

        return {
            broadcast_id: data.Items[0]["broadcast_id"],
            connection_ids: ("connection_ids" in data.Items[0] ? data.Items[0]["connection_ids"].values : []),
            broadcast_name: data.Items[0]["broadcast_name"],
            telegram_chat_id: data.Items[0]["telegram_chat_id"],
            locale: data.Items[0]["locale"],
            time_zone: data.Items[0]["time_zone"]
        };
    } catch (error) {
        console.error(error);
        await SNSPublish("FollowersConnectionBroadcastList", `${error}`);
        return undefined;
    }
}

exports.handler = async (event, context) => {
    //console.log(event);
    //console.log(context);
    const response = {
        statusCode: 200,
        body: JSON.stringify({}),
    };

    const stage = Alias(context);

    let FunctionName = 'Replikanto-Broadcast';
    if (stage !== undefined) {
        FunctionName = FunctionName + ":" + stage;
    }
    //console.log(FunctionName);

    let broadcast_promisses = [];

    //for (let i = 0; i < event.Records.length; i++) { // pegar sempre a última informação da posição ao invés de event.Records.length
        //const record = event.Records[i];
        const record = event.Records[event.Records.length - 1];
        //console.log(record);
        const event_name = record.eventName;
        const dynamobd = record.dynamodb;
        const broadcast_list_id = dynamobd.Keys.broadcast_list_id.S;
        //console.log("broadcast_list_id", broadcast_list_id);

        let position_info = undefined;
        if (event_name === "MODIFY" || event_name === "INSERT") {
            position_info = DynamoDB.Converter.unmarshall(dynamobd.NewImage);
            console.log(position_info);
            try {
                delete position_info.broadcast_list_id;
            } catch (ignored) {
            }
            
            let followersConnections = await FollowersConnectionBroadcastList(dynamo, broadcast_list_id);
            if (followersConnections === undefined) {
                //continue;
                return response;
            }

            const connections = followersConnections.connection_ids;

            for (let i = 0; i < connections.length; i=i+slice_size) {
                const connections_sliced = connections.slice(i, i+slice_size);
                broadcast_promisses.push(lambda.invokeAsync({
                    FunctionName,
                    InvokeArgs: JSON.stringify({
                        connections: connections_sliced,
                        action: "position_info",
                        payload: {
                            id: followersConnections.broadcast_id,
                            name: followersConnections.broadcast_name,
                            positions: position_info
                        },
                        broadcast_list_id
                    })
                }).promise());
            }

            if (followersConnections.telegram_chat_id !== undefined && followersConnections.telegram_chat_id !== 0) {
                let position_description = "";
                Object.keys(position_info)
                    .forEach(function eachKey(key, idx, array) {
                        const payload = position_info[key];
                        if (payload !== undefined && typeof payload === "object") {
                            var lastUpdateDate = new Date(payload.last_update);

                            const locale = followersConnections.locale;
                            const timeZone = followersConnections.time_zone;
                            let localeDateString = lastUpdateDate.toLocaleString();
                            if (locale !== undefined && timeZone != undefined) {
                                localeDateString = lastUpdateDate.toLocaleString(locale, {timeZone});
                            }
                            //const connected = payload.connected;

                            //let description = `Instrument: *${key}*\nQuantity: ${payload.quantity}\nMarket Position: ${payload.market_position}\nAverage Price: ${payload.average_price}\nLast Update: ${localeDateString}\n${connected ? "Connected" : "Disconnected"}`;
                            let description = `Instrument: *${key}*\nQuantity: ${payload.quantity}\nMarket Position: ${payload.market_position}\nAverage Price: ${payload.average_price}\nLast Update: ${localeDateString}`;
                            if (idx === array.length - 1) {
                                position_description = position_description + description;
                            } else {
                                position_description = position_description + description + "\n---\n";
                            }
                        }
                });
                if (!position_description) { // position_description was empty string, false, 0, null, undefined, ...
                    position_description = "No information about positions";
                }
                await SNSPublish("Replikanto Broadcast Position(s)", `${followersConnections.broadcast_name}\n---\n${position_description}`, followersConnections.telegram_chat_id);
            }
        } else if (event_name === "REMOVE") {
            
        }

    //}

    if (broadcast_promisses.length > 0) {
        await Promise.all(broadcast_promisses); 
    }

    return response;
};
