var isPrime = function (num) {
    if (num < 2)
        return false;
    for (var i = 2; i < num; i++) {
        if (num % i == 0)
            return false;
    }
    return true;
};

var doWork = function (jobAssignement) {
    // the actual work
    jobAssignement.result = [];

    for (var i = jobAssignement.parameters.from; i <= jobAssignement.parameters.to; i++) {
        if (isPrime(i))
            jobAssignement.result.push(i);
    }
};
//# sourceMappingURL=primes.js.map
