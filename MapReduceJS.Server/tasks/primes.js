var core = require('./../server.core');

/**
* A simple MapReduce task calculating primes.
*/
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

        var job = new core.Job();
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
//# sourceMappingURL=primes.js.map
