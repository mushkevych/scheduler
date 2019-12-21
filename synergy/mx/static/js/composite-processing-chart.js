const margin = {top: 100, right: 40, bottom: 40, left: 100};
const width = 1440;
const height = 720;

const x = d3.scaleBand().range([0, width]);
const y = d3.scaleBand().range([0, height]);
const z = d3.scaleLinear().domain([0, 16]).clamp(true);
const c = d3.scaleOrdinal(d3.schemeCategory10).domain(d3.range(10));

const svg = d3.select("body").append("svg")
    .attr("width", document.body.clientWidth)
    .attr("height", document.body.clientHeight)
    .style("margin-left", margin.left + "px")
    .append("g")
    .attr("width", document.body.clientWidth)
    .attr("height", document.body.clientHeight)
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


function renderCompositeProcessingChart(miserables, mx_trees, jobs, num_days) {  // mx_trees, job_matrix, uow_matrix) {
    const datetimeUtcNow = new Date();
    let processNames = [];
    const n = (num_days + 1) * 24;
    let timeperiods = d3.range(n).map(function (i) {
        const datetime = new Date(datetimeUtcNow.getTime());
        datetime.setUTCHours(datetime.getUTCHours() - i);
        return dateToTimeperiod(datetime);
    });
    timeperiods = timeperiods.reverse();

    // matrix[timeperiod][process_name] = {job_details}
    const matrix = [];

    // Compile list of process names
    for (const [mx_tree_name, tree_obj] of Object.entries(mx_trees)) {
        processNames = processNames.concat(tree_obj.sorted_process_names);
    }

    // build matrix with dimensions <timeperiods * process_names>
    for (let i = 0; i < n; i++) {
        matrix[i] = d3.range(processNames.length).map(
            function (j) {
                return {x: i, y: j, z: 0, timeperiod: timeperiods[i], process_name: processNames[j], state: null};
            }
        );
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
            // matrix[timeperiod_index][process_index].process_name = job_obj.process_name;
            // matrix[timeperiod_index][process_index].timeperiod = job_obj.timeperiod;
            matrix[timeperiod_index][process_index].state = job_obj.state;
            matrix[timeperiod_index][process_index].z += job_obj.state.length;
        }
    }

    // initialize X and Y axis with domain of possible values
    x.domain(timeperiods);
    y.domain(processNames);

    svg.append("rect")
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
        });

    row.append("line")
        .attr("x2", width); //horizontal line

    row.append("text")
        .attr("x", -6)
        .attr("y", y.bandwidth() / 2)
        .attr("dy", ".32em")
        .attr("text-anchor", "end")
        .text(function (d, i) {
            return processNames[i];
        });

    const column = svg.selectAll(".column")
        .data(matrix).enter()
        .append("g")
        .attr("class", "column")
        .attr("transform", function (d, i) {
            const timeperiod = timeperiods[i];
            return "translate(" + x(timeperiod) + ", 0)";
        })
        .each(build_column);

    column
        .append("line")
        .attr("x1", -height); //vertical line

    column.append("g")
        .append("text")
        .attr("x", 6)
        .attr("y", y.bandwidth() / 2)
        .attr("dy", ".32em")
        .attr("text-anchor", "start")
        .text(function (d, i) {
            return timeperiods[i];
        })
        .attr("transform", "rotate(-90)");

    function build_column(column) {
        const cell = d3.select(this).selectAll(".cell")
            .data(column)
            .enter().append("rect")
            .attr("class", "cell")
            .attr("x", function (d) {
                const timeperiod = timeperiods[d.x];
                return x(timeperiod);
            })
            .attr("y", function (d) {
                const process_name = processNames[d.y];
                return y(process_name);
            })
            .attr("width", x.bandwidth())
            .attr("height", y.bandwidth())
            .style("fill-opacity", function (d) {
                return z(d.z);
            })
            .style("fill", function (d) {
                return matrix[d.x][d.y].z ? c(matrix[d.x][d.y].z) : null;
                // return matrix[d.x][d.y].state === "state_embryo" ? c(matrix[d.x][d.y].z) : null;
            })
            .on("mouseover", mouseover)
            .on("mouseout", mouseout);
    }

    function mouseover(p) {
        d3.selectAll(".row text").classed("active", function (d, i) {
            return i === p.y;
        });
        d3.selectAll(".column text").classed("active", function (d, i) {
            return i === p.x;
        });
    }

    function mouseout() {
        d3.selectAll("text").classed("active", false);
    }
}
