const https = require('https');

exports.handler = async (event) => {
    const payload = JSON.parse(event['Records'][0]['Sns']['Message']);
    const subject = event['Records'][0]['Sns']['Subject'];
    
    const chat_id = "" + payload.chat_id;
    const token = payload.token;

    //if (chat_id !== "-1001715515414") {
        //return {
            //statusCode: 200
        //};
    //}

    const dataObj = {
        chat_id: chat_id, 
        text: `*${subject}* - ${payload.msg}`,
        parse_mode: "markdown" // or html
    };
    
    const data = jsonEncode(dataObj);

    return httpsrequest(data, token).then(() => {
        console.log(dataObj.text);
        return {
            statusCode: 200
        };
    }).catch(error => {
        const errorMsg = error.message;
        console.error(errorMsg, dataObj.text);
        let statusCode = errorMsg.match(/\d/g);
        statusCode = statusCode.join("");
        return {
            statusCode
        };
    });
};

function httpsrequest(data, token) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'api.telegram.org',
            port: 443,
            path: '/bot'+token+'/sendMessage',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': data.length
            }
        };

        const req = https.request(options, (res) => {
            if (res.statusCode < 200 || res.statusCode >= 300) {
                return reject(new Error('statusCode=' + res.statusCode));
            }
            var body = [];
            res.on('data', function(chunk) {
                body.push(chunk);
            });
            res.on('end', function() {
                try {
                    body = JSON.parse(Buffer.concat(body).toString());
                } catch(e) {
                    reject(e);
                }
                resolve(body);
            });
        });

        req.on('error', (e) => {
            reject(e.message);
        });

        req.write(data);
        req.end();
    });
}

function jsonEncode(value) {
    return JSON.stringify(value).replace(/[\u0080-\uFFFF]/g, function(match) {
        return '\\u' + ('0000' + match.charCodeAt(0).toString(16)).slice(-4);
    });
}