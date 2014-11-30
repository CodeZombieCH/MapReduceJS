// Worker
// Supposed to run as a web worker thread


// Import socket.io
importScripts('/socket.io/socket.io.js');


/* Interfaces -------------------------------------------------------------- */
interface IJobAssignment {
	jobId: number;
	parameters: any;
	result: any;
}


/* Worker ------------------------------------------------------------------ */

var log = (message) => console.log('worker #' + workerId + ':', message);

log('preparing web worker');

var workerId;
var socket = io();

//socket.on('connect', log('Connected'));
//socket.on('connect_error', (data) => {
//	log('Connection failed' + data);s
//});

socket.on('workerReady', (data) => {
	workerId = data.workerId;
	log('ready for work');

	// Get a job
	socket.emit('getJob');
});

socket.on('getJob', (jobAssignement: IJobAssignment) => {
	if(!jobAssignement) {
		log('No more jobs left. Halting now');
		return;
	}

	log('job received');
	log(jobAssignement.parameters);

	// Job recieved, do something usefull
	log('working...');
	doWork(jobAssignement);
	log('done');

	// Job completed, emit result
	socket.emit('completeJob', jobAssignement);
});

socket.on('completeJob', (data) => {
	log('job completed');
	// Server recieved result --> start over

	// Get a job
	socket.emit('getJob');
});


// Get the wheels turning...
log('get the wheels turning')
socket.emit('workerReady');


/* The actual work --------------------------------------------------------- */
var isPrime = (num) => {
	if(num < 2) return false;
	for(var i = 2; i < num; i++) {
		if(num % i == 0)
			return false;
	}
	return true;
};

var doWork = (jobAssignement: IJobAssignment) => {
	// the actual work

	jobAssignement.result = [];

	for(var i = jobAssignement.parameters.from; i <= jobAssignement.parameters.to; i++) {
		if(isPrime(i))
			jobAssignement.result.push(i);
	}
}
