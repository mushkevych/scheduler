const margin = {top: 80, right: 40, bottom: 40, left: 80};
const width = 1024;
const height = 720;

const x = d3.scaleBand().range([0, width]);
const z = d3.scaleLinear().domain([0, 4]).clamp(true);
const c = d3.scaleOrdinal(d3.schemeCategory10).domain(d3.range(10));

const svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .style("margin-left", margin.left + "px")
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


function renderCompositeProcessingChart(miserables, mx_trees, jobs, num_days) {  // mx_trees, job_matrix, uow_matrix) {
    let process_names = [];
    let timeperiods = num_days * 24;
    // const n = timeperiods;

    // matrix[process_name][timeperiod] = job_details
    const matrix = [];
    const nodes = miserables.nodes;
    const n = nodes.length;

    // Compile list of process names
    for (const [mx_tree_name, tree_obj] of Object.entries(mx_trees)) {
        process_names = process_names.concat(tree_obj.sorted_process_names);
    }

    // Compute index per node.
    nodes.forEach(
        function (node, i) {
            node.index = i;
            node.count = 0;
            matrix[i] = d3.range(n).map(function (j) {
                return {x: j, y: i, z: 0};
            });
        }
    );

    // Convert links to matrix; count character occurrences.
    miserables.links.forEach(function (link) {
        matrix[link.source][link.target].z += link.value;
        matrix[link.target][link.source].z += link.value;
        matrix[link.source][link.source].z += link.value;
        matrix[link.target][link.target].z += link.value;
    });

    // Precompute the order.
    const orders = {
        name: d3.range(n).sort(function (a, b) {
            return d3.ascending(nodes[a].name, nodes[b].name);
        })
    };

    // The default sort order.
    x.domain(orders.name);

    svg.append("rect")
        .attr("class", "background")
        .attr("width", width)
        .attr("height", height);

    const row = svg.selectAll(".row")
        .data(matrix)
        .enter().append("g")
        .attr("class", "row")
        .attr("transform", function (d, i) {
            return "translate(0," + x(i) + ")";
        })
        .each(build_row);

    row.append("line")
        .attr("x2", width);

    row.append("text")
        .attr("x", -6)
        .attr("y", x.bandwidth() / 2)
        .attr("dy", ".32em")
        .attr("text-anchor", "end")
        .text(function (d, i) {
            return nodes[i].name;
        });

    const column = svg.selectAll(".column")
        .data(matrix)
        .enter().append("g")
        .attr("class", "column")
        .attr("transform", function (d, i) {
            return "translate(" + x(i) + ")rotate(-90)";
        });

    column.append("line")
        .attr("x1", -width);

    column.append("text")
        .attr("x", 6)
        .attr("y", x.bandwidth() / 2)
        .attr("dy", ".32em")
        .attr("text-anchor", "start")
        .text(function (d, i) {
            return nodes[i].name;
        });

    function build_row(row) {
        const cell = d3.select(this).selectAll(".cell")
            .data(row.filter(function (d) {
                return d.z;
            }))
            .enter().append("rect")
            .attr("class", "cell")
            .attr("x", function (d) {
                return x(d.x);
            })
            .attr("width", x.bandwidth())
            .attr("height", x.bandwidth())
            .style("fill-opacity", function (d) {
                return z(d.z);
            })
            .style("fill", function (d) {
                return nodes[d.x].group === nodes[d.y].group ? c(nodes[d.x].group) : null;
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
