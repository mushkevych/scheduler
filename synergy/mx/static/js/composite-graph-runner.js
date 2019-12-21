const tasks = [
    {
        "startDate": new Date("Sun Dec 18 01:36:45 EST 2019"),
        "endDate": new Date("Sun Dec 18 02:36:45 EST 2019"),
        "taskName": "E Job",
        "status": "RUNNING"
    }];

const taskStatus = {
    "SUCCEEDED": "bar",
    "FAILED": "bar-failed",
    "RUNNING": "bar-running",
    "KILLED": "bar-killed"
};

const taskNames = ["D Job", "P Job", "E Job", "A Job", "N Job"];

tasks.sort(function(a, b) {
    return a.endDate - b.endDate;
});
const maxDate = tasks[tasks.length - 1].endDate;
tasks.sort(function(a, b) {
    return a.startDate - b.startDate;
});
const minDate = tasks[0].startDate;

let format = "%H:%M";
const timeDomainString = "1day";

const gantt = d3.gantt().taskTypes(taskNames).taskStatus(taskStatus).tickFormat(format);

const margin = {
    top: 20,
    right: 40,
    bottom: 80,
    left: 40
};
gantt.margin(margin);

gantt.timeDomainMode("fixed");
changeTimeDomain(timeDomainString);

gantt(tasks);

function changeTimeDomain(timeDomainString) {
    switch (timeDomainString) {
        case "6hr":
            format = "%H:%M";
            gantt.timeDomain([ d3.timeHour.offset(getEndDate(), -6), getEndDate() ]);
            break;

        case "1 day":
            format = "%Y%m%dT%H";
            gantt.timeDomain([ d3.timeDay.offset(getEndDate(), -1), getEndDate() ]);
            break;

        case "7 days":
            format = "%Y%m%dT%H";
            gantt.timeDomain([ d3.timeDay.offset(getEndDate(), -7), getEndDate() ]);
            break;

        case "30 days":
            format = "%Y%m%dT%H";
            gantt.timeDomain([ d3.timeDay.offset(getEndDate(), -30), getEndDate() ]);
            break;
        default:
            format = "%H:%M"

    }
    gantt.tickFormat(format);
    gantt.redraw(tasks);
}

function getEndDate() {
    let lastEndDate = Date.now();
    if (tasks.length > 0) {
        lastEndDate = tasks[tasks.length - 1].endDate;
    }

    return lastEndDate;
}

function addTask() {

    const lastEndDate = getEndDate();
    const taskStatusKeys = Object.keys(taskStatus);
    const taskStatusName = taskStatusKeys[Math.floor(Math.random() * taskStatusKeys.length)];
    const taskName = taskNames[Math.floor(Math.random() * taskNames.length)];

    tasks.push({
        "startDate" : d3.timeHour.offset(lastEndDate, Math.ceil(1 * Math.random())),
        "endDate" : d3.timeHour.offset(lastEndDate, (Math.ceil(Math.random() * 3)) + 1),
        "taskName" : taskName,
        "status" : taskStatusName
    });

    changeTimeDomain(timeDomainString);
    gantt.redraw(tasks);
}

function removeTask() {
    tasks.pop();
    changeTimeDomain(timeDomainString);
    gantt.redraw(tasks);
}
