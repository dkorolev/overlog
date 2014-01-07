function CircularBuffer(n) {
    this.xs = new Array(n || 100);
    this.empty = true;
    this.begin = 0;
    this.end = 0;
};

CircularBuffer.prototype.push = function(x) {
    this.xs[this.end] = x;
    if (!this.empty && this.begin === this.end) {
        this.end = (this.end + 1) % this.xs.length;
        this.begin = this.end;
    } else {
        this.end = (this.end + 1) % this.xs.length;
    }
    this.empty = false;
};

CircularBuffer.prototype.dump = function() {
    var result = [];
    if (!this.empty) {
        var first = true;
        for (var i = this.begin; first || i != this.end; i = (i + 1) % this.xs.length) {
            result.push(this.xs[i]);
            first = false;
        }
    }
    return result;
};

CircularBuffer.prototype.size = function() {
    if (this.empty) {
        return 0;
    } else if (this.begin === this.end) {
        return this.xs.length;
    } else {
        return this.end;
    }
};

CircularBuffer.prototype.peek_least_recent = function() {
    if (this.empty) {
        throw new Error('Called peek_least_recent() on empty CircularBuffer.');
    } else {
        return this.xs[this.begin];
    }
};

CircularBuffer.prototype.peek_most_recent = function() {
    if (this.empty) {
        throw new Error('Called peek_most_recent() on empty CircularBuffer.');
    } else {
        return this.xs[(this.end + this.xs.length - 1) % this.xs.length];
    }
};

module.exports = CircularBuffer;
