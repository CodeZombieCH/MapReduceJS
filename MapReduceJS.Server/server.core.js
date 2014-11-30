var uuid = require('node-uuid');

var ClientJobAssignment = (function () {
    function ClientJobAssignment(assignmentId, jobId, parameters, result) {
        this.assignmentId = assignmentId;
        this.jobId = jobId;
        this.parameters = parameters;
        this.result = result;
    }
    return ClientJobAssignment;
})();
exports.ClientJobAssignment = ClientJobAssignment;

/**
* A map reduce job supposed to be completed by 1 or more workers
*/
var Job = (function () {
    function Job() {
        this.assignments = [];
        this.status = 0 /* Ready */;
    }
    Job.prototype.hasCompleted = function () {
        for (var i = 0; i < this.assignments.length; i++) {
            var assignment = this.assignments[i];

            // Skip completed assignments
            if (!assignment.completed)
                return false;
        }

        return true;
    };

    Job.prototype.applyResults = function () {
        /*
        * TODO: Compare all individual results calculated by the workers
        * If the are the same, assume result is valid. If not, do whatever makes sense :P
        */
        // For now, we just take the result of the first worker
        this.result = this.assignments[0].result;
    };
    return Job;
})();
exports.Job = Job;

/**
* TODO: Make immutable
*/
var JobAssignment = (function () {
    function JobAssignment(job) {
        this.job = job;
        this.assignmentId = uuid.v4();
    }
    Object.defineProperty(JobAssignment.prototype, "AssignmentId", {
        get: function () {
            return this.assignmentId;
        },
        enumerable: true,
        configurable: true
    });

    Object.defineProperty(JobAssignment.prototype, "Job", {
        get: function () {
            return this.job;
        },
        enumerable: true,
        configurable: true
    });

    JobAssignment.prototype.assignWorker = function (worker) {
        this.assigned = new Date();
        this.worker = worker;
    };

    JobAssignment.prototype.hasTimedOut = function (assignmentTTL) {
        return (this.assigned.getTime() + assignmentTTL * 1000 < new Date().getTime());
    };

    JobAssignment.prototype.getClientAssignment = function () {
        return new ClientJobAssignment(this.assignmentId, this.job.jobId, this.job.parameters, undefined);
    };
    return JobAssignment;
})();
exports.JobAssignment = JobAssignment;

var Worker = (function () {
    function Worker(workerId) {
        this.workerId = workerId;
    }
    return Worker;
})();
exports.Worker = Worker;

(function (Status) {
    Status[Status["Ready"] = 0] = "Ready";
    Status[Status["Assigned"] = 1] = "Assigned";
    Status[Status["Completed"] = 2] = "Completed";
})(exports.Status || (exports.Status = {}));
var Status = exports.Status;

var Scheduler = (function () {
    function Scheduler(task) {
        this.task = task;
        this.assignSingleJobToNWorker = 1;
        this.maxActiveJobs = 20;
        this.assignmentTTL = 30;
        this.activeJobs = [];
        this.completedJobs = [];
    }
    Scheduler.prototype.getJob = function (worker) {
        /*
        * Might be a better idea to assign the latest job to workers until assignSingleJobToNWorker
        * is reached. At this time, a new job is requested.
        * Negative side effect: high chance same job to same client
        *
        * TODO: Make sure the same job is not assigned to the same client twice
        */
        var job;
        if (this.activeJobs.length < this.maxActiveJobs) {
            // Spawn new job
            job = this.task.createJob();

            if (!job) {
                // TODO: Handle completed
                console.log('Scheduler: no more jobs left for the current task');
                return null;
            }

            this.activeJobs.push(job);

            // Get an assignment
            var assignment = this.getJobAssignment(job, worker);
            return assignment;
        } else {
            for (var i = 0; i < this.activeJobs.length; i++) {
                var job = this.activeJobs[i];

                // Try to get an assignment
                var assignment = this.getJobAssignment(job, worker);

                if (assignment)
                    return assignment;
            }

            // If we got here, no active job has an assignment free
            if (!this.task.hasJobs()) {
                throw 'completed';
            } else {
                throw 'jobs drained';
            }
        }
    };

    Scheduler.prototype.completeJob = function (jobAssignment, result) {
        // Check if timed out
        if (jobAssignment.hasTimedOut(this.assignmentTTL)) {
            console.log('Assignment timed out. Ignoring assignment.');
            return;
        }

        jobAssignment.completed = new Date();
        jobAssignment.result = result;

        // Update job
        var job = jobAssignment.Job;
        if (job.hasCompleted()) {
            job.applyResults();

            // Tell the task the job has completed, so it can store the results
            this.task.completeJob(job);

            this.activeJobs.remove(job);
            this.completedJobs.push(job);
        }
    };

    Scheduler.prototype.getJobAssignment = function (job, worker) {
        // Check if we have enough redundancy (= workers working at the same job)
        if (job.assignments.length < this.assignSingleJobToNWorker) {
            // Create a new assignment
            var assignment = new JobAssignment(job);
            assignment.assignWorker(worker);
            job.assignments.push(assignment);
            return assignment;
        }

        for (var i = 0; i < job.assignments.length; i++) {
            var assignment = job.assignments[i];

            // Skip completed assignments
            if (assignment.completed)
                continue;

            // Check TTL
            if (assignment.hasTimedOut(this.assignmentTTL)) {
                // Timed out
                // Assign new worker
                assignment.worker = worker;
                assignment.assigned = new Date();
                return assignment;
            }
        }

        // No assignments found
        return null;
    };
    return Scheduler;
})();
exports.Scheduler = Scheduler;

var PrimesMapReduceTask = (function () {
    function PrimesMapReduceTask(from, to, chunkSize) {
        this.from = from;
        this.to = to;
        this.chunkSize = chunkSize;
        this.current = 0;
    }
    PrimesMapReduceTask.prototype.createJob = function () {
        if (!this.hasJobs())
            return null;

        console.log('Task: creating job');

        var job = new Job();
        job.jobId = this.current;
        job.parameters = { from: this.current, to: this.current + this.chunkSize - 1 };

        this.current += this.chunkSize;

        return job;
    };

    PrimesMapReduceTask.prototype.completeJob = function (job) {
        console.log('Task: completing job');
        console.log('Task: highest prime number of job: ' + job.result[job.result.length - 1]);
        // TODO: Persist, reduce, whatever you need
    };

    PrimesMapReduceTask.prototype.hasJobs = function () {
        return (this.current + this.chunkSize) <= this.to;
    };
    return PrimesMapReduceTask;
})();
exports.PrimesMapReduceTask = PrimesMapReduceTask;


Array.prototype.random = function () {
    return this[Math.floor(Math.random() * this.length)];
};

Array.prototype.remove = function (value) {
    var index = this.indexOf(value);
    if (index != -1) {
        return this.splice(index, 1);
    }
    return undefined;
};
//# sourceMappingURL=server.core.js.map
