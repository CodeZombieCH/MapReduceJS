// Manager
// Supposed to run on the main thread


// Interfaces
interface IWorker {
	workerId: string;
}

// Extend navigator
interface Navigator {
	hardwareConcurrency: number;
}

/**
 * Worker manager running in the main thread,
 * starting web workers that do the actual work.
 */
class WorkerManager {
	private workers: Worker[] = [];

	constructor() {
		this.initWorkers();
	}

	/**
	 * Creates and starts workers
	 */
	private initWorkers() {
		var cores = navigator.hardwareConcurrency || 4;

		console.log('creating workers...');

		for(var i = 0; i < cores; i++) {
			// Create new web workers
			var webWorker = new Worker('webworker.js')

			this.workers.push(webWorker);
		}

		console.log('workers created');
	}

	/**
	* Terminates all workers
	*/
	public terminateAll() {
		for(var i = 0; i < this.workers.length; i++) {
			this.workers[i].terminate();
		}
	}
}


var workerManager = new WorkerManager();

/**
 * Convenience method to stop all worker
 */
function stop() {
	workerManager.terminateAll();
}