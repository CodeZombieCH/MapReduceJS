// Manager
// Supposed to run on the main thread


/**
* Worker manager running in the main thread,
* starting web workers that do the actual work.
*/
var WorkerManager = (function () {
    function WorkerManager() {
        this.workers = [];
        this.initWorkers();
    }
    /**
    * Creates and starts workers
    */
    WorkerManager.prototype.initWorkers = function () {
        var cores = navigator.hardwareConcurrency || 4;

        console.log('creating workers...');

        for (var i = 0; i < cores; i++) {
            // Create new web workers
            var webWorker = new Worker('webworker.js');

            this.workers.push(webWorker);
        }

        console.log('workers created');
    };

    /**
    * Terminates all workers
    */
    WorkerManager.prototype.terminateAll = function () {
        for (var i = 0; i < this.workers.length; i++) {
            this.workers[i].terminate();
        }
    };
    return WorkerManager;
})();

var workerManager = new WorkerManager();

/**
* Convenience method to stop all worker
*/
function stop() {
    workerManager.terminateAll();
}
//# sourceMappingURL=client.js.map
