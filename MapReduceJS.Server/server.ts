/// <reference path="typings/socket.io/socket.io.d.ts" />
/// <reference path="typings/node/node.d.ts" />


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
	console.log('    by Marc-André Bühler');
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



var scheduler = new core.Scheduler(new core.PrimesMapReduceTask(0, 100000000, 100000));


// Socket.io
var workers = {};
var count = 0;

io.on('connection', function(socket) {

	socket.on('workerReady', function (message) {
		console.log('worker ready');

		// we store the newly assigned worker id in the socket session for this worker
		//var worker = socket.worker = new core.Worker(uuid.v4());
		var worker = socket.worker = new core.Worker(socket.id);

		// add the client's username to the global list
		workers[worker.workerId] = worker;

		socket.emit('workerReady', socket.worker);

		socket.broadcast.emit('statsUpdate', Object.keys(workers).length);
	});

	socket.on('getJob', function(message) {
		console.log('worker #' + socket.worker.workerId + ' is requesting a job');

		console.log('assigning job...');

		var jobAssignment = scheduler.getJob(socket.worker);
		socket.assignment = jobAssignment;

		if(jobAssignment) {
			console.log('assigned job #' + jobAssignment.AssignmentId);
			socket.emit('getJob', jobAssignment.getClientAssignment());
		}
		else {
			console.log('no more jobs left');
			socket.emit('getJob', undefined);
		}
	});

	socket.on('completeJob', function(clientJobAssignement: core.IClientJobAssignment) {
		console.log('worker #' + socket.worker.workerId + ' is completing a job');

		// Retrieve full blown assignment from session
		var jobAssignment = <core.JobAssignment>socket.assignment;

		// Validate assignment id
		if(jobAssignment.AssignmentId != clientJobAssignement.assignmentId) {
			throw 'invalid assignment id';
		}

		scheduler.completeJob(jobAssignment, clientJobAssignement.result);

		socket.emit('completeJob');
	});

	socket.on('stats', function() {
		socket.emit('stats', Object.keys(workers).length);
	});

	socket.on('disconnect', function() {
		console.log('worker #' + (socket.worker && socket.worker.workerId || socket.id) + ' disconnected');

		delete workers[socket.id];

		socket.broadcast.emit('statsUpdate', Object.keys(workers).length);
	});
});

process.on('SIGINT', function () {
	console.log("\nGracefully shutting down from SIGINT (Ctrl-C)");
	shutdown()
	process.exit();
})

process.on('SIGTERM', function() {
	console.log("\nGracefully shutting down from SIGTERM");
	shutdown()
	process.exit();
})

function shutdown() {
	// some other closing procedures go here
}
