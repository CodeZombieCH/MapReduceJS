// Setup basic express server
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var port = 3000//process.env.PORT || 3000;
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



var scheduler = new core.Scheduler(new core.PrimesMapReduceTask(0, 1000000, 100000));


// Socket.io
var workers = {};
var count = 0;

io.on('connection', function(socket) {

	socket.on('workerReady', function (message) {
		console.log('worker ready');

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

		var jobAssignment = scheduler.getJob(socket.worker);
		socket.assignment = jobAssignment;

		socket.emit('getJob', jobAssignment.getClientAssignment());
		//socket.emit('getJob', { parameters: { from: count, to: (count += 100000) } });
		
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
