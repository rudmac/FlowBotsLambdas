const ApiGatewayManagementApi = require('aws-sdk/clients/apigatewaymanagementapi');

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

    let ret = {};
    try {
        ret = await callbackAPI
            .postToConnection({ ConnectionId: connection_id, Data: JSON.stringify(data) })
            .promise();
        
        return true;
    } catch (e) {
        console.error("sendToConnection Error", connection_id, ret, e);
        return false;
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
    //await Promise.all(connections.map(async (connection) => {
    //connections.map(async (connection) => {
    connections.forEach(async (connection) => {
        const connection_obj = JSON.parse(connection);
        //console.log(connection_obj);
        let promise = new Promise(async (resolve, reject) => {
            const response = await sendToConnection(stage, connection_obj.connection_id, connection_obj.region, {
                action,
                payload,
                replikanto_version,
                broadcast_id
            });
            resolve({
                connection_id: connection_obj.connection_id,
                status: response ? "200" : "500"
            });
        });
        promisses.push(promise);
    });
    //}));
    
    let responses = await Promise.all(promisses);
    console.log("Responses", responses);

    const response = {
        statusCode: 200,
        body: JSON.stringify(responses),
    };
    
    return response;
};