"use strict";

let fs = require('fs');
let path = require('path');
require('console.table');

let summaries = {};
let tables = {};
let processed = 0;

let dataDir = ".";
if(process.argv.length === 3) {
    dataDir = process.argv[2];
}
let notFound = 0;

let metricTitles = {
    pub: "Publisher Metrics",
    sub: "Subscriber Metrics",
    pubsub: "Publish+Subscriber Metrics",
    reqrep: "Request Reply Metrics",
};

let files = fs.readdirSync(dataDir);
let dataFiles = files.filter(function(v) {
    return v.endsWith(".csv");
});


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
    fs.readFile(file, (err, data) => {
        if(err) {
            throw err;
        }
        let lines = data.toString().split('\n');

        // csv data is: metric, count, millis, date, version
        lines.forEach((line, index) => {
            if(index === 0 || line === "") {
                // csv header
                return;
            }
            // split the fields
            let f = line.split(',');
            let e = {
                metric: f[0],
                count: parseInt(f[1], 10),
                millis: parseInt(f[2], 10),
                date: Date.parse(f[3]),
                version: f[4]
            };
            e.rate = (e.count * 1000) / e.millis;

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
            let versions = Object.keys(tables[metric].versions);
            versions.forEach(function(version) {
                // reduce the samples for a version
                let samples = tables[metric].versions[version];
                let summary = {count: 0, millis: 0, version: version, metric: metric, max: 0, min: 0, samples: 0, lat: 0, rate: 0};
                summary = samples.reduce((accumulator, sample, index) => {
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
                summary.rate = summary.count * 1000 / summary.millis;
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
    return Math.floor(s.count*1000 / s.millis).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + " msgs/sec";
}


function print() {
    Object.keys(summaries).forEach(function(metric) {
        console.log(metricTitles[metric].toUpperCase(),"\n");
        // sort by performance
        summaries[metric].sort(function(a, b){
            return (b.count*1000 / b.millis) - (a.count*1000 / a.millis);
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
