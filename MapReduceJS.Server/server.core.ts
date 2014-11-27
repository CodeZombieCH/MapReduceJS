export class Job {
	public jobId: number;
	public parameters: any;
	public assignments: JobAssignment[];
	public status: Status;

	constructor() {
		this.assignments = [];
		this.status = Status.Ready;
	}
}

export class JobAssignment {
	public assigned: Date;
	public completed: Date;
	public timedOut: Date;
	public result: any;

	constructor(public job: Job, public worker: Worker) {
		this.assigned = new Date();
	}
}

export class Worker {
	constructor(public workerId: string) {
	}
}

export enum Status {
	Ready,
	Assigned,
	Completed
}

export class Scheduler {
	private assignSingleJobToNWorker: number = 3;
	private maxActiveJobs: number = 5;
	private assignmentTTL: number = 30; // Seconds

	private activeJobs: Job[];
	private completedJobs: Job[];

	constructor(private task: IMapReduceTask) {
		this.activeJobs = [];
		this.completedJobs = [];
	}

	public getJob(worker: Worker): JobAssignment {

		/*
		 * Might be a better idea to assign the latest job to workers until assignSingleJobToNWorker
		 * is reached. At this time, a new job is requested.
		 * Negative side effect: high chance same job to same client
		 * 
		 * TODO: Make sure the same job is not assigned to the same client twice
		 */

		var job: Job;
		if(this.activeJobs.length < this.maxActiveJobs) {
			// Spawn new job
			job = this.task.createJob();

			if(!job) {
				// TODO: Handle completed
				throw 'completed';
			}

			this.activeJobs.push(job);

			// Get an assignment
			var assignment = this.getJobassignment(job, worker);
			return assignment;
		}
		else {
			// v1: Get random job from active jobs
			/*do {
				job = this.activeJobs.random();

				// Try to get an assignment
				var assignment = this.getJobassignment(job, worker);
			}
			while(assignment == null);
			*/

			// v2: Use round robin
			// TODO: Implement round robin
			for(var i = 0; i < this.activeJobs.length; i++) {
				var job = this.activeJobs[i];

				// Try to get an assignment
				var assignment = this.getJobassignment(job, worker);

				if(assignment) return assignment;
			}

			// If we got here, no active job has an assignment free
			if(!this.task.hasJobs()) {
				// TODO: Handle completed
				throw 'completed';
			}
			else {
				throw 'jobs drained';
			}
		}
	}

	public completeJob(job: JobAssignment) {

	}

	private getJobassignment(job: Job, worker: Worker) : JobAssignment {
		// Check if we have enough redundancy (= workers working at the same job)
		if(job.assignments.length < this.assignSingleJobToNWorker) {
			// Create a new assignment
			var assignment = new JobAssignment(job, worker);
			job.assignments.push(assignment);
			return assignment;
		}

		// Try to find an outdated assignment
		for(var i = 0; i < job.assignments.length; i++) {
			var assignment = job.assignments[i];

			// Skip completed assignments
			if(assignment.completed) continue;

			// Check TTL
			if(assignment.assigned.getTime() + this.assignmentTTL < new Date().getTime()) {
				// Timed out
				// Assign new worker
				assignment.worker = worker;
				assignment.assigned = new Date();
				return assignment;
			}
		}

		// No assignments found
		return null;
	}
}

export interface IMapReduceTask {
	createJob(): Job;
	hasJobs(): boolean;
}

export class PrimesMapReduceTask implements IMapReduceTask {
	private current: number = 0;

	constructor(private from: number, public to: number, private chunkSize: number) {
	}

	public createJob(): Job {
		if(!this.hasJobs()) return null;

		var job = new Job();
		job.jobId = this.current;
		job.parameters = { from: this.current, to: this.current + this.chunkSize - 1 };

		this.current += this.chunkSize;

		return job;
	}

	public hasJobs() : boolean {
		return (this.current + this.chunkSize) < this.to;
	}	
}


// Utils
interface Array<T> {
	random(): T;
}
Array.prototype.random = function() {
	return this[Math.floor(Math.random() * this.length)]
}
