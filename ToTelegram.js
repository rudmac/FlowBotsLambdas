const https = require('https');

//const chat_id = process.env.CHAT_ID;
//const token = process.env.TOKEN;

exports.handler = async (event) => {
    const payload = JSON.parse(event['Records'][0]['Sns']['Message']);
    const subject = event['Records'][0]['Sns']['Subject'];
    
    const chat_id = "" + payload.chat_id;
    const token = payload.token;

    const data = jsonEncode({
        chat_id: chat_id, 
        text: `*${subject}* - ${payload.msg}`,
        parse_mode: "markdown" // or html
    });
    
    console.log(data.text);

    return httpsrequest(data, token).then(() => {
        const response = {
            statusCode: 200
        };
        return response;
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
                return reject(new Error('statusCode = ' + res.statusCode));
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