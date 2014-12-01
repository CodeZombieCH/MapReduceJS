// Worker
// Supposed to run as a web worker thread
// Import socket.io
importScripts('/socket.io/socket.io.js');

// Load task
// TODO: Return as parameter when worker registers at the server
importScripts('/tasks/primes.js');


/* Worker ------------------------------------------------------------------ */
var log = function (message) {
    return console.log('worker #' + workerId + ':', message);
};

log('preparing web worker');

var workerId;
var socket = io();

//socket.on('connect', log('Connected'));
//socket.on('connect_error', (data) => {
//	log('Connection failed' + data);s
//});
socket.on('workerReady', function (data) {
    workerId = data.workerId;
    log('ready for work');

    // Get a job
    socket.emit('getJob');
});

socket.on('getJob', function (jobAssignement) {
    if (!jobAssignement) {
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

socket.on('completeJob', function (data) {
    log('job completed');

    // Server recieved result --> start over
    // Get a job
    socket.emit('getJob');
});

// Get the wheels turning...
log('get the wheels turning');
socket.emit('workerReady');
//# sourceMappingURL=webworker.js.map
