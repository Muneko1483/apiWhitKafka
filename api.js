var express = require('express');
var kafka = require('kafka-node');
var app = express();

var bodyParser = require('body-parser')
app.use( bodyParser.json() );       
app.use(bodyParser.urlencoded({ 
  extended: true
})); 

var Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);
    
    Consumer = kafka.Consumer,
    client = new kafka.Client(),
    consumer = new Consumer(client,
        [{ topic: 'Posts', offset: 0}],
        {
            autoCommit: false
        }
    );

consumer.on('message', function (message) {
    console.log(message);
});

consumer.on('error', function (err) {
    console.log('Error:',err);
})

consumer.on('offsetOutOfRange', function (err) {
    console.log('offsetOutOfRange:',err);
})
producer.on('ready',function(){ 
    console.log('Producer is ready');
    });


producer.on('error',function(err){
    console.log('Producer is in error state');
    console.log(err);
})    

app.get('/', function(req,res){
    res.json({greeting:'------'})
    });

app.post('/sendMsg',function(req,res){
    var sentMessage = JSON.stringify(req.body.message);
    payloads = [
        { topic: req.body.topic, messages:sentMessage , partition: 0 }
    ];
    producer.send(payloads, function (err, data) {
            res.json(data);
    });
    
});
app.listen(5001,function(){
    console.log('kafka producer running at 5001')
});
