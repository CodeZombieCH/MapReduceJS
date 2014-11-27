// Setup basic express server
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var port = process.env.PORT || 3000;
var uuid = require('node-uuid');

import core = require('./server.core');

server.listen(port, function () {
	console.log('');
	console.log('       Map Reduce JS');
	console.log('   by Marc-Andre Buehler');
	console.log('');
	console.log('    /\\_| |_/\\    | |');
	console.log('    \\       / /\\/   \\/\\');
	console.log('   _/       \\_\\       /');
	console.log('  |_    _    _|   _   |_');
	console.log('   _|  (_)  |_   (_)   _|');
	console.log('  |_         _|       |');
	console.log('    \\       / /       \\');
	console.log('    / _   _ \\ \\/\\   /\\/');
	console.log('    \\/ |_| \\/    |_|');
	console.log('');

	console.log('Server listening at port %d', port);
});

// Expose client
app.use(express.static(__dirname + '/../MapReduceJS.Client'));

// Expose simple test client
app.use('/test', express.static(__dirname + '/../client/test'));

// Lib
function guid() {
	return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
		var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
		return v.toString(16);
	});
};



var scheduler = new core.Scheduler(new core.PrimesMapReduceTask(0, 100000, 1000000));


// Socket.io
var workers = {};
var count = 0;

io.on('connection', function(socket) {

	socket.on('workerReady', function (message) {
		console.log('worker ready');

		//var workerId = guid();
		var workerId = uuid.v4();

		// we store the newly assigned worker id in the socket session for this worker
		socket.worker = new core.Worker(workerId);

		// add the client's username to the global list
		workers[workerId] = {
			workerId: workerId
		};

		socket.emit('workerReady', socket.worker);
	});

	socket.on('getJob', function(message) {
		console.log('worker #' + socket.worker.workerId + ' is requesting a job');

		console.log('assigning job');

		//var jobAssignement = scheduler.getJob(socket.worker);
		//socket.emit('getJob', { parameters: jobAssignement.job.parameters });

		socket.emit('getJob', { parameters: { from: count, to: (count += 100000) } });
		
	});

	socket.on('completeJob', function (data) {
		console.log('completing job');
		console.log('highest prime number of job: ' + data.result[data.result.length - 1]);
		socket.emit('completeJob');
	});
});

process.on('SIGINT', function () {
	console.log("\nGracefully shutting down from SIGINT (Ctrl-C)");
	// some other closing procedures go here
	process.exit();
})
