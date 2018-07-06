"use strict";

var fs = require('fs');
var path = require('path');
require('console.table');

var summaries = {};
var tables = {};
var processed = 0;

var dataDir = ".";
if(process.argv.length === 3) {
    dataDir = process.argv[2];
}
var notFound = 0;

var dataFiles = ["pub.csv", "pubsub.csv", "reconnect.csv", "rr.csv", "sub.csv"];
dataFiles.forEach(function(v) {
    parse(path.resolve(path.join(dataDir, v)));
});

// parse the cvs files
function parse(file) {
    if(! fs.existsSync(file)) {
        notFound++;
        return;
    }
    console.log(file);
    fs.readFile(file, function(err, data) {
        if(err) {
            throw err;
        }
        var lines = data.toString().split('\n');

        // csv data is: metric, count, millis, date, version
        lines.forEach(function(line, index){
            if(index === 0 || line === "") {
                // csv header
                return;
            }
            // split the fields
            var f = line.split(',');
            var e = {
                metric: f[0],
                count: parseInt(f[1], 10),
                millis: parseInt(f[2], 10),
                date: Date.parse(f[3]),
                version: f[4]
            };

            if(e.metric === 'rr' && f.length === 6) {
                e.lat = parseFloat(f[5])
            }

            // organize by metric 'pub', 'sub', etc
            if(!tables[e.metric]) {
                tables[e.metric] = {
                    versions: {}
                };
            }
            // organize by version
            if(!tables[e.metric].versions[e.version]) {
                tables[e.metric].versions[e.version] = [];
            }
            tables[e.metric].versions[e.version].push(e);
        });
        loaded();
    });
}


function loaded() {
    processed++;
    if(processed + notFound === dataFiles.length) {
        // we parsed all the sample, generate a summary
        Object.keys(tables).forEach(function(metric) {
            var versions = Object.keys(tables[metric].versions);
            versions.forEach(function(version) {
                // reduce the samples for a version
                var samples = tables[metric].versions[version];
                var summary = {count: 0, millis: 0, version: version, metric: metric, average: 0, max: 0, min: 0, samples: 0, lat: 0};
                summary = samples.reduce(function(accumulator, sample, index) {
                    if(index === 0) {
                        accumulator.min = sample.millis;
                    }
                    if(accumulator.samples === 0) {
                        accumulator.samples = sample.count;
                    }
                    accumulator.count += sample.count;
                    accumulator.millis += sample.millis;
                    accumulator.max = Math.max(accumulator.max, sample.millis);
                    accumulator.min = Math.min(accumulator.min, sample.millis);

                    if(sample.lat) {
                        accumulator.lat += sample.lat;
                    }
                    return accumulator;
                }, summary);
                // calculate an average
                summary.average = summary.millis / samples.length;
                if(summary.lat) {
                    summary.lat = summary.lat / samples.length;
                } else {
                    delete summary.lat;
                }
                if(!summaries[metric]) {
                    summaries[metric] = [];
                }

                // organize the results by metrics
                summaries[metric].push(summary);
            });
        });
        print();
    }
}

function rate(s) {
    if(s.metric !== 'reconnect') {
        return Math.floor(s.samples / (s.average / 1000)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + " msgs/sec";
    } else {
        return Math.floor(s.average) + "ms";
    }


}


function print() {
    Object.keys(summaries).forEach(function(metric) {
        console.log(metric.toUpperCase());
        // sort by peformance
        summaries[metric].sort(function(a, b){
            return a.average - b.average;
        });
        summaries[metric].forEach(function(s){
            s.rate = rate(s);
            delete s.count;
            delete s.millis;
            delete s.metric;
        });
        console.table(summaries[metric]);

    });
}

function units(metric) {
    return " msgs/sec";
}
