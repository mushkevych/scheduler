const MIN_RECT_SIDE_SIZE = 28;
const margin = {top: 100, right: 40, bottom: 40, left: 120};
let width;
let height;

// scales: https://www.d3indepth.com/scales
let x;
let y;
const c = d3.scaleOrdinal()
    .domain(["state_embryo", "state_in_progress", "state_processed", "state_final_run",
        "state_skipped", "state_noop", "state_inconsistent", "state_inactive"])
    .range(["#d5f5ff", "#c3fdb8", "#15c200", "#adff2f",
        "#ff8a65", "#ffa", "#d28aff", "#d3d3d3"])
    .unknown("pink");

const svg = d3.select("body").append("svg")
    .attr("class", "composite-chart")
    .attr("width", document.body.clientWidth)
    .attr("height", document.body.clientHeight)
    .style("margin-left", "10px")
    .style("margin-right", "10px");

const inner = svg.append("g")
    .attr("width", document.body.clientWidth)
    .attr("height", document.body.clientHeight)
    .attr("transform", `translate(${margin.left}, ${margin.top})`);

// Set up zoom support
const zoom = d3.zoom().on('zoom', function () {
    inner.attr("transform", d3.event.transform);
});
svg.call(zoom);

// div for the tooltip
const divTooltip = d3.select("body").append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

// div for the Job tile
const divJobTile = d3.select("body").append("div")
    .style("opacity", 0);

let isJobTileShown = false;


function hideDivTile() {
    divJobTile.transition().
        duration(200).
        style("opacity", 0).
        style("pointer-events", "none");

    isJobTileShown = false;
}


function showDivTile(p) {
    divJobTile.transition()
        .duration(200)
        .style("opacity", 1)
        .style("pointer-events", "all")
        .attr("class", `${p.state} job-tile`);

    const treeHref = `<a href="/scheduler/${p.mx_page}&#35;${p.tree_name}">${p.tree_name}</a>`;
    divJobTile.html(`
        <a class='close-button' onclick='hideDivTile()'></a>
        ${p.process_name}<br/>
        ${p.timeperiod}<br/>
        ${p.state}<br/>
        url: ${treeHref}<br/>
        qualifier: ${p.time_qualifier}<br/>
        grouping: ${p.time_grouping}`)
            .style("left", (d3.event.pageX + 5) + "px")
            .style("top", (d3.event.pageY - 28) + "px");

    // TODO: reuse mx_page::infoJobTile to build Job Tile
    // infoJobTile(p, divJobTile, false, false);
}


function initChartDimensions(num_timeperiods, num_processes) {
    width = num_timeperiods * MIN_RECT_SIDE_SIZE;
    height = num_processes * MIN_RECT_SIDE_SIZE;

    x = d3.scaleBand().range([0, width]);
    y = d3.scaleBand().range([0, height]);
}


function findJob(process_name, timeperiod, jobs) {
    let resp = {};
    for (const jobObj of jobs[process_name]) {
        if (jobObj.timeperiod !== timeperiod) {
            continue;
        }
        resp = jobObj;
        break;
    }
    return resp;
}


function findTree(process_name, mx_trees) {
    let resp = {};
    for (const [mx_tree_name, treeObj] of Object.entries(mx_trees)) {
        if (!treeObj.sorted_process_names.includes(process_name)) {
            continue;
        }
        resp = treeObj;
        break;
    }
    return resp;
}


function renderCompositeProcessingChart(processNames, timeperiods, jobs, mx_trees) {
    // build matrix with dimensions <process_names * timeperiods>
    // matrix[process_name][timeperiod] = {job_details}
    const matrix = [];
    for (let i = 0; i < processNames.length; i++) {
        matrix[i] = [];
        for (let j = 0; j < timeperiods.length; j++) {
            const jobObj = findJob(processNames[i], timeperiods[j], jobs);
            const treeObj = findTree(processNames[i], mx_trees);
            const processObj = treeObj.processes[processNames[i]];
            matrix[i][j] = {
                x: j, y: i, z: 0,
                timeperiod: timeperiods[j],
                process_name: processNames[i],
                state: jobObj.state,
                mx_page: treeObj.mx_page,
                tree_name: treeObj.tree_name,
                time_qualifier: processObj.time_qualifier,
                time_grouping: processObj.time_grouping,
            };
        }
    }

    // initialize X and Y axis with domain of possible values
    x.domain(timeperiods);
    y.domain(processNames);

    inner.append("g")
        .append("rect")
        .attr("class", "background")
        .attr("width", width)
        .attr("height", height);

    const row = inner.selectAll(".row")
        .data(matrix).enter()
        .append("g")
        .attr("class", "row")
        .attr("transform", function (d, i) {
            const process_name = processNames[i];
            return `translate(0, ${y(process_name)})`;
        })
        .each(build_row);

    row.append("g")
        .append("line")
        .attr("x2", width); //horizontal line

    row.append("g")
        .append("text")
        .attr("x", -6)
        .attr("y", y.bandwidth() / 2)
        .attr("dy", ".32em")
        .attr("text-anchor", "end")
        .text(function (d, i) {
            return processNames[i];
        });

    const column = inner.selectAll(".column")
        .data(timeperiods).enter()
        .append("g")
        .attr("class", "column")
        .attr("transform", function (d, i) {
            const timeperiod = timeperiods[i];
            return `translate(${x(timeperiod)}, 0)`;
        });

    column.append("g")
        .append("line")
        .attr("x1", -height)
        .attr("class", function (d) {
            // assign styles from composite-processing-chart.css to lines dividing days, months and years
            if (d.endsWith("000000")) {
                return "yearly-timeperiod";
            } else if (d.endsWith("0000")) {
                return "monthly-timeperiod";
            } else if (d.endsWith("00")) {
                return "daily-timeperiod";
            } else {
                return "";
            }
        })
        .attr("transform", "rotate(-90)"); //vertical line

    column.append("g")
        .append("text")
        .attr("x", 6)
        .attr("y", y.bandwidth() / 2)
        .attr("dy", y.bandwidth() / 8)
        .attr("text-anchor", "start")
        .text(function (d, i) {
            return timeperiods[i];
        })
        .attr("transform", "rotate(-90)");

    function build_row(row) {
        const cell = d3.select(this).selectAll(".cell")
            .data(row).enter()
            .append("g")
            .append("rect")
            .attr("class", "cell")
            .attr("class", function (d) {
                // instead of CSS coloring is performed via d3.scaleOrdinal
                // return matrix[d.y][d.x].state;
            })
            .attr("x", function (d) {
                const timeperiod = timeperiods[d.x];
                return x(timeperiod);
            })
            .attr("y", function (d) {
                // each row is build on top of horizontal line,
                // as such - there is no need to put additional horizontal spacer
                return 0;
            })
            .attr("width", x.bandwidth())
            .attr("height", y.bandwidth())
            .style("fill-opacity", function (d) {
                return matrix[d.y][d.x].state ? 100 : 0;
            })
            .style("fill", function (d) {
                return c(matrix[d.y][d.x].state);
            })
            .on("mouseover", mouseover)
            .on("mouseout", mouseout)
            .on("click", mouseclick);
    }

    function mouseover(p) {
        // highlight process_name and timeperiod
        d3.selectAll(".row text").classed("active", function (d, i) {
            return i === p.y;
        });
        d3.selectAll(".column text").classed("active", function (d, i) {
            return i === p.x;
        });

        if (isJobTileShown) {
            // do not show tooltip if the Job Tile is shown
            return;
        }

        // show tooltip
        divTooltip.transition()
            .duration(200)
            .style("opacity", .9);
        const treeHref = `<a href="/scheduler/${p.mx_page}&#35;${p.tree_name}">${p.tree_name}</a>`;
        divTooltip.html(`
            ${p.process_name}<br/>
            ${p.timeperiod}<br/>
            ${p.state}<br/>
            url: ${treeHref}<br/>
            qualifier: ${p.time_qualifier}<br/>
            grouping: ${p.time_grouping}`)
                .style("left", (d3.event.pageX + 5) + "px")
                .style("top", (d3.event.pageY - 28) + "px");
    }

    function mouseout() {
        if (!isJobTileShown) {
            // de-highlight process_name and timeperiod
            d3.selectAll("text").classed("active", false);
        }

        // hide tooltip
        divTooltip.transition()
            .duration(200)
            .style("opacity", 0);
    }

    function mouseclick(p) {
        // do not show any new tooltip until Job Tile is shown and ...
        isJobTileShown = true;
        // ... hide existing tooltip
        mouseout();

        showDivTile(p);
    }
}
