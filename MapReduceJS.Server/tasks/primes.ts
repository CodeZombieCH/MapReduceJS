import core = require('./../server.core');

/**
 * A simple MapReduce task calculating primes.
 */
export class PrimesMapReduceTask implements core.IMapReduceTask {
	private current: number = 0;

	constructor(private from: number, public to: number, private chunkSize: number) {
	}

	public createJob(): core.Job {
		if(!this.hasJobs()) return null;

		console.log('Task: creating job')

		var job = new core.Job();
		job.jobId = this.current;
		job.parameters = { from: this.current, to: this.current + this.chunkSize - 1 };

		this.current += this.chunkSize;

		return job;
	}

	public completeJob(job: core.Job) {
		console.log('Task: completing job')
		console.log('Task: highest prime number of job: ' + job.result[job.result.length - 1]);

		// TODO: Persist, reduce, whatever you need
	}

	public hasJobs(): boolean {
		return (this.current + this.chunkSize) <= this.to;
	}
}
