var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var kafka = require('kafka-node');

// Kafka topic and options for the consumer
// replace with the topic and the private IP of
// one of your Kafka broker hosts
var topic = 'mytopic';
var brokerHost = '20.0.0.13:9092';
// consumerOpts used by consumer
var consumerOpts = {
    kafkaHost: brokerHost,
    groupId: 'mygroupid',
    protocol: ['roundrobin'] // how to read from partitions
}
// Create the consumer
var consumer = new kafka.ConsumerGroup(consumerOpts, topic);
// Create the producer
var client = new kafka.KafkaClient({kafkaHost: brokerHost});
var producer = new kafka.Producer(client, {requireAcks:1});

// Handle requests for '/'
app.get('/', function(req,res) {
    res.sendFile(__dirname + '/index.html');
});

// Handle connections from a browser
io.on('connection', function(socket) {
    console.log('browser connected');

    // Handle messages received through the consumer
    consumer.on('message', function(message) {
        console.log(message);
        // emit the message to any browser clients
        io.emit('message', message.value);
    });

    // If an error happens with the consumer, log it to the console
    consumer.on('error', function(err) {
        console.log('error', err);
    });

    // Send messages submitted through the form
    socket.on('send message', function(msg) {
        console.log('Sending message: ' + msg);
        // Construct the payload
        payloads=[
            { topic: topic, messages: [ msg ] }
        ];
        // Send to Kafka
        producer.send(payloads, function(err, data) {
            console.log(data)
        });
    });

    // handle disconnects
    socket.on('disconnect', function() {
        console.log('browser disconnected');
    });
});

// listen on port 80
http.listen(80), function(){
    console.log('listening on *:80')
}