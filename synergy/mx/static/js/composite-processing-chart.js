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
    .range(["#d5f5ff", "#c3fdb8", "#15c200", "#adff2f", "#ff8a65", "#0e0e0e", "#ffa", "#d28aff", "#d3d3d3"])
    .unknown("black");

const svg = d3.select("body").append("svg")
    .attr("class", "composite-chart")
    .attr("width", document.body.clientWidth)
    .attr("height", document.body.clientHeight)
    .style("margin-left", margin.left + "px")
    .append("g")
    .attr("width", document.body.clientWidth)
    .attr("height", document.body.clientHeight)
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

// Set up zoom support
const zoom = d3.zoom().on('zoom', function () {
    inner.attr("transform", d3.event.transform);
});
svg.call(zoom);

// div for the tooltip
const divTooltip = d3.select("body").append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);


function initChartDimensions(num_timeperiods, num_processes) {
    width = num_timeperiods * MIN_RECT_SIDE_SIZE;
    height = num_processes * MIN_RECT_SIDE_SIZE;

    x = d3.scaleBand().range([0, width]);
    y = d3.scaleBand().range([0, height]);
}


function renderCompositeProcessingChart(processNames, timeperiods, jobs) {
    // build matrix with dimensions <process_names * timeperiods>
    // matrix[process_name][timeperiod] = {job_details}
    const matrix = [];
    for (let i = 0; i < processNames.length; i++) {
        matrix[i] = [];
        for (let j = 0; j < timeperiods.length; j++) {
            matrix[i][j] = {x: j, y: i, z: 0, timeperiod: timeperiods[j], process_name: processNames[i], state: null};
        }
    }

    // Assign job properties
    for (const [process_name, job_objects] of Object.entries(jobs)) {
        let process_index = processNames.indexOf(process_name);

        for (let j = 0; j < job_objects.length; j++) {
            const job_obj = job_objects[j];
            const timeperiod_index = timeperiods.indexOf(job_obj.timeperiod);
            if (timeperiod_index === -1) {
                continue;
            }
            matrix[process_index][timeperiod_index].state = job_obj.state;
        }
    }

    // initialize X and Y axis with domain of possible values
    x.domain(timeperiods);
    y.domain(processNames);

    svg.append("g")
        .append("rect")
        .attr("class", "background")
        .attr("width", width)
        .attr("height", height);

    const row = svg.selectAll(".row")
        .data(matrix).enter()
        .append("g")
        .attr("class", "row")
        .attr("transform", function (d, i) {
            const process_name = processNames[i];
            return "translate(0, " + y(process_name) + ")";
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

    const column = svg.selectAll(".column")
        .data(timeperiods).enter()
        .append("g")
        .attr("class", "column")
        .attr("transform", function (d, i) {
            const timeperiod = timeperiods[i];
            return "translate(" + x(timeperiod) + ", 0)";
        });

    column.append("g")
        .append("line")
        // .attr("class", "line")
        .attr("x1", -height)

        .attr("stroke", function (d) {
            // divide days with a colored line
            return d.endsWith("00") ? "red" : "";
        })
        .attr("stroke-width", function (d) {
            // divide days with a thicker line
            return d.endsWith("00") ? 2 : 1;
        })
        .attr("fill", function (d) {
            // divide days with a colored line
            return d.endsWith("00") ? "purple" : "none";
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
                // and such - there is no need to put additional horizontal spacer
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
            .on("mouseout", mouseout);
    }

    function mouseover(p) {
        // highlight process_name and timeperiod
        d3.selectAll(".row text").classed("active", function (d, i) {
            return i === p.y;
        });
        d3.selectAll(".column text").classed("active", function (d, i) {
            return i === p.x;
        });

        // show tooltip
        divTooltip.transition()
            .duration(200)
            .style("opacity", .9);
        divTooltip.html(p.process_name + "<br/>" + p.timeperiod + "<br/>" + p.state)
            .style("left", (d3.event.pageX) + "px")
            .style("top", (d3.event.pageY - 28) + "px");
    }

    function mouseout() {
        // de-highlight process_name and timeperiod
        d3.selectAll("text").classed("active", false);

        // hide tooltip
        divTooltip.transition()
            .duration(500)
            .style("opacity", 0);
    }
}
