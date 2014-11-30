/// <reference path="typings/chalk/chalk.d.ts" />

var uuid = require('node-uuid');
var chalk = require('chalk');

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
	/**
	 * A unique job identifier
	 */
	public jobId: number;
	/**
	 * Parameters of the job
	 */
	public parameters: any;
	/**
	 * Assignments currently assigned to workers
	 */
	public assignments: JobAssignment[];
	/**
	 * Status of the job
	 */
	public status: Status;
	/**
	 * The result of the job
	 */
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
	/**
	 * Job redundancy
	 * Defines the number of workers completing the same job until it
	 * is marked as completed.
	 */
	private assignSingleJobToNWorker: number = 1;
	/**
	 * The maximum number of active jobs
	 */
	private maxActiveJobs: number = 4;
	/**
	 * The time to live of an assignment in seconds. After this time elapses,
	 * the assignment is marked as timed out.
	 */
	private assignmentTTL: number = 20; // Seconds

	private activeJobs: Job[];
	private completedJobs: Job[];

	constructor(private task: IMapReduceTask, maxActiveJobs: number, assignmentTTL: number) {
		this.maxActiveJobs = maxActiveJobs;
		this.assignmentTTL = assignmentTTL;

		this.activeJobs = [];
		this.completedJobs = [];
	}

	/**
	 * Get a job assignment for the current task
	 */
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
				console.log('Scheduler: no more jobs left for the current task');
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
				console.log(chalk.green('Scheduler: no more jobs left for the current task'));
				return null;
			}
			else {
				console.log(chalk.red('Scheduler: job queue drained'));
				return null;
			}
		}
	}

	/**
	 * Report the completion of a job assignment
	 */
	public completeJob(jobAssignment: JobAssignment, result: any) {
		// Check if timed out
		if(jobAssignment.hasTimedOut(this.assignmentTTL)) {
			console.log(chalk.yellow(
				'Assignment timed out. Ignoring assignment. ('
				+ (new Date().getTime() - jobAssignment.assigned.getTime()) / 1000 + 's' 
				+ ' > ' + this.assignmentTTL
				+ 's) '
			));
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
