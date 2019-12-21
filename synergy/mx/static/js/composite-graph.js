// function renderCompositeChart(trees, time_window_days) {

d3.gantt = function () {
    const FIT_TIME_DOMAIN_MODE = "fit";
    const FIXED_TIME_DOMAIN_MODE = "fixed";

    let margin = {
        top: 20,
        right: 40,
        bottom: 80,
        left: 40
    };
    let radius = 1;
    let baseClass = "";
    let selector = "body";
    let timeDomainStart = d3.timeDay.offset(new Date(), -3);
    let timeDomainEnd = d3.timeHour.offset(new Date(), +3);
    let timeDomainMode = FIT_TIME_DOMAIN_MODE;  // fixed or fit
    let taskTypes = [];
    let taskStatus = [];
    let innerHeight = document.body.clientHeight - margin.top - margin.bottom;
    let innerWidth = document.body.clientWidth - margin.right - margin.left;
    let height = innerHeight - 5;
    let width = innerWidth - 5 ;
    let pageHeight = document.body.clientHeight;
    let pageWidth = document.body.clientWidth;

    let tickFormat = "%H:%M";

    let keyFunction = function (d) {
        return d.startDate + d.taskName + d.endDate;
    };

    const rectTransform = function (d) {
        return "translate(" + x(d.startDate) + "," + y(d.taskName) + ")";
    };

    let x;          // x axis labels
    let y;          // y axis labels
    let xAxis;      // x axis line
    let yAxis;      // y axis line
    let xAxisGrid;  // horizontal gridlines
    let yAxisGrid;  // vertical gridlines


    const initTimeDomain = function (tasks) {
        if (timeDomainMode === FIT_TIME_DOMAIN_MODE) {
            if (tasks === undefined || tasks.length < 1) {
                timeDomainStart = d3.timeDay.offset(new Date(), -3);
                timeDomainEnd = d3.timeHour.offset(new Date(), +3);
                return;
            }
            tasks.sort(function (a, b) {
                return a.endDate - b.endDate;
            });
            timeDomainEnd = tasks[tasks.length - 1].endDate;
            tasks.sort(function (a, b) {
                return a.startDate - b.startDate;
            });
            timeDomainStart = tasks[0].startDate;
        }
    };

    const initGrid = function () {
        // * the negative chart height and width to the tickSize functions ensures that the axis lines will span across the chart.
        // * an empty string to tickFormat ensures that tick labels aren’t rendered
        // * ticks function specifies the number of tick marks, here set to 10 to equal the count on the main axes
        xAxisGrid = d3.axisBottom(x).tickSize(-innerHeight).tickFormat("");  //.ticks(10);
        yAxisGrid = d3.axisLeft(y).tickSize(-innerWidth).tickFormat("");     //.ticks(10);
    };

    const initAxis = function () {
        x = d3.scaleTime().domain([timeDomainStart, timeDomainEnd]).range([0, width]).clamp(true);
        y = d3.scaleBand().domain(taskTypes).range([0, height]).padding(.1);

        xAxis = d3.axisBottom(x)
            .tickFormat(d3.timeFormat(tickFormat))
            .tickSize(8)
            .tickPadding(8);

        yAxis = d3.axisLeft(y)
            .tickSize(0);
    };

    function gantt(tasks) {

        initTimeDomain(tasks);
        initAxis();
        initGrid();

        const svg = d3.select(selector)
            .append("svg")
            .attr("class", "chart")
            .attr("width", pageWidth)
            .attr("height", pageHeight)
            .append("g")
            .attr("class", "gantt-chart")
            .attr("width", pageWidth)
            .attr("height", pageHeight)
            .attr("transform", "translate(" + margin.left + ", " + margin.top + ")");

        svg.selectAll(".chart")
            .data(tasks, keyFunction).enter()
            .append("rect")
            .attr("rx", radius)
            .attr("ry", radius)
            .attr("class", function (d) {
                if (taskStatus[d.status] == null) {
                    return "bar";
                }
                return taskStatus[d.status];
            })
            .attr("y", 0)
            .attr("transform", rectTransform)
            .attr("height", function (d) {
                return y.bandwidth();
            })
            .attr("width", function (d) {
                return Math.max(1, (x(d.endDate) - x(d.startDate)));
            });

        if (baseClass) {
            selectors.classed(baseClass, true);
        }

        // gridlines
        svg.append("g")
            .attr("class", "x axis-grid")
            .attr("transform", "translate(0," + innerHeight + ")")
            .call(xAxisGrid);
        svg.append("g")
            .attr("class", "y axis-grid")
            .call(yAxisGrid);

        // axis
        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0, " + height + ")")
            .transition()
            .call(xAxis)
            // rotating x label
            .selectAll("text")
            .style("text-anchor", "start")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(45)")
        ;
        svg.append("g")
            .attr("class", "y axis")
            .transition();

        return gantt;
    }

    gantt.redraw = function (tasks) {

        initTimeDomain(tasks);
        initAxis();
        initGrid();

        const svg = d3.select(".chart");

        const ganttChartGroup = svg.select(".gantt-chart");
        const rect = ganttChartGroup.selectAll("rect").data(tasks, keyFunction);

        rect.enter()
            .insert("rect", ":first-child")
            .attr("rx", radius)
            .attr("ry", radius)
            .attr("class", function (d) {
                if (taskStatus[d.status] == null) {
                    return "bar";
                }
                return taskStatus[d.status];
            })
            .transition()
            .attr("y", 0)
            .attr("transform", rectTransform)
            .attr("height", function (d) {
                return y.bandwidth();
            })
            .attr("width", function (d) {
                return Math.max(1, (x(d.endDate) - x(d.startDate)));
            });

        rect.transition()
            .attr("transform", rectTransform)
            .attr("height", function (d) {
                return y.bandwidth();
            })
            .attr("width", function (d) {
                return Math.max(1, (x(d.endDate) - x(d.startDate)));
            });

        rect.exit().remove();

        // gridlines
        svg.select(".x.axis-grid").call(xAxisGrid);
        svg.select(".y.axis-grid").call(yAxisGrid);

        // axis
        svg.select(".x.axis").transition().call(xAxis)
            .selectAll("text")
            .style("text-anchor", "start")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(45)");
        svg.select(".y.axis").transition().call(yAxis);

        return gantt;
    };

    gantt.margin = function (value) {
        if (!arguments.length)
            return margin;
        margin = value;
        return gantt;
    };

    gantt.timeDomain = function (value) {
        if (!arguments.length)
            return [timeDomainStart, timeDomainEnd];
        timeDomainStart = +value[0];
        timeDomainEnd = +value[1];
        return gantt;
    };

    /**
     * @param {string}
     *                value The value can be "fit" - the domain fits the data or
     *                "fixed" - fixed domain.
     */
    gantt.timeDomainMode = function (value) {
        if (!arguments.length)
            return timeDomainMode;
        timeDomainMode = value;
        return gantt;

    };

    gantt.taskTypes = function (value) {
        if (!arguments.length)
            return taskTypes;
        taskTypes = value;
        return gantt;
    };

    gantt.taskStatus = function (value) {
        if (!arguments.length)
            return taskStatus;
        taskStatus = value;
        return gantt;
    };

    gantt.width = function (value) {
        if (!arguments.length)
            return width;
        width = +value;
        return gantt;
    };

    gantt.height = function (value) {
        if (!arguments.length)
            return height;
        height = +value;
        return gantt;
    };

    gantt.tickFormat = function (value) {
        if (!arguments.length)
            return tickFormat;
        tickFormat = value;
        return gantt;
    };

    gantt.selector = function (value) {
        if (!arguments.length)
            return selector;
        selector = value;
        return gantt;
    };

    gantt.radius = function (value) {
        if (!arguments.length)
            return radius;
        radius = value;
        return gantt;
    };

    gantt.baseClass = function (value) {
        if (!arguments.length)
            return baseClass;
        baseClass = value;
        return gantt;
    };

    gantt.keyFunction = function (value) {
        if (!arguments.length)
            return keyFunction;
        keyFunction = value;
        return gantt;
    };

    return gantt;
};
