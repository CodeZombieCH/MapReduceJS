var uuid = require('node-uuid');


export interface IClientJobAssignment {
	assignmentId: string;
	jobId: number;
	parameters: any;
	result: any;
}

export class ClientJobAssignment implements IClientJobAssignment {
	constructor(public assignmentId: string,
		public jobId: number,
		public parameters: any,
		public result: any) {
	}
}

/**
 * A map reduce job supposed to be completed by 1 or more workers
 */
export class Job {
	public jobId: number;
	public parameters: any;
	/**
	 * Jobs currently assigned to workers
	 */
	public assignments: JobAssignment[];
	public status: Status;
	public result: any;

	constructor() {
		this.assignments = [];
		this.status = Status.Ready;
	}

	hasCompleted(): boolean {
		for(var i = 0; i < this.assignments.length; i++) {
			var assignment = this.assignments[i];

			// Skip completed assignments
			if(!assignment.completed) return false;
		}

		return true;
	}

	applyResults() {
		/*
		 * TODO: Compare all individual results calculated by the workers
		 * If the are the same, assume result is valid. If not, do whatever makes sense :P
		*/

		// For now, we just take the result of the first worker
		this.result = this.assignments[0].result;
	}
}

/**
 * TODO: Make immutable
 */
export class JobAssignment {
	private assignmentId: string;
	private job: Job
	public worker: Worker
	public assigned: Date;
	public completed: Date;
	public timedOut: Date;
	public result: any;

	get AssignmentId(): string {
		return this.assignmentId;
	}

	get Job(): Job {
		return this.job;
	}

	constructor(job: Job) {
		this.job = job;
		this.assignmentId = uuid.v4();
	}

	assignWorker(worker: Worker) {
		this.assigned = new Date();
		this.worker = worker;
	}

	hasTimedOut(assignmentTTL: number) {
		return (this.assigned.getTime() + assignmentTTL*1000 < new Date().getTime());
	}

	getClientAssignment(): IClientJobAssignment {
		return new ClientJobAssignment(
			this.assignmentId,
			this.job.jobId,
			this.job.parameters,
			undefined
		);
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
	private assignSingleJobToNWorker: number = 1;
	private maxActiveJobs: number = 20;
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
				console.log('Scheduler: no more jobs left for the current task')
				return null;
			}

			this.activeJobs.push(job);

			// Get an assignment
			var assignment = this.getJobAssignment(job, worker);
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
				var assignment = this.getJobAssignment(job, worker);

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

	public completeJob(jobAssignment: JobAssignment, result: any) {
		// Check if timed out
		if(jobAssignment.hasTimedOut(this.assignmentTTL)) {
			console.log('Assignment timed out. Ignoring assignment. (' + (new Date().getTime() - jobAssignment.assigned.getTime())/1000 + 's)');
			return;
		}

		jobAssignment.completed = new Date();
		jobAssignment.result = result;

		// Update job
		var job = jobAssignment.Job;
		if(job.hasCompleted()) {
			job.applyResults();

			// Tell the task the job has completed, so it can store the results
			this.task.completeJob(job);

			this.activeJobs.remove(job);
			this.completedJobs.push(job);
		}
	}

	private getJobAssignment(job: Job, worker: Worker) : JobAssignment {
		// Check if we have enough redundancy (= workers working at the same job)
		if(job.assignments.length < this.assignSingleJobToNWorker) {
			// Create a new assignment
			var assignment = new JobAssignment(job);
			assignment.assignWorker(worker);
			job.assignments.push(assignment);
			return assignment;
		}

		// Try to find an timed out assignment
		for(var i = 0; i < job.assignments.length; i++) {
			var assignment = job.assignments[i];

			// Skip completed assignments
			if(assignment.completed) continue;

			// Check TTL
			if(assignment.hasTimedOut(this.assignmentTTL)) {
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
	completeJob(job: Job);
	hasJobs(): boolean;
}

export class PrimesMapReduceTask implements IMapReduceTask {
	private current: number = 0;

	constructor(private from: number, public to: number, private chunkSize: number) {
	}

	public createJob(): Job {
		if(!this.hasJobs()) return null;

		console.log('Task: creating job')

		var job = new Job();
		job.jobId = this.current;
		job.parameters = { from: this.current, to: this.current + this.chunkSize - 1 };

		this.current += this.chunkSize;

		return job;
	}

	public completeJob(job: Job) {
		console.log('Task: completing job')
		console.log('Task: highest prime number of job: ' + job.result[job.result.length - 1]);

		// TODO: Persist, reduce, whatever you need
	}

	public hasJobs() : boolean {
		return (this.current + this.chunkSize) <= this.to;
	}
}


// Utils
interface Array<T> {
	random(): T;
	remove(item: T): T
}

Array.prototype.random = function() {
	return this[Math.floor(Math.random() * this.length)]
}

Array.prototype.remove = function(value) {
	var index = this.indexOf(value);
	if(index != -1) {
		return this.splice(index, 1);
	}
	return undefined;
}
