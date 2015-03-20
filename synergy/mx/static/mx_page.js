// The grid manages tiles using ids, which you can define. For our
// examples we'll just use the tile number as the unique id.
var TILE_IDS = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31
];


function fillArray(value, len) {
    var arr = [];
    for (var i = 0; i < len; i++) {
        arr.push(value);
    }
    return arr;
}

// templates in JSON matching the predefined selections you can
// choose on the demo page
var DemoTemplateRows = [
    fillArray(" . ", 3),   // yearly
    fillArray(" . ", 12),  // monthly
    fillArray(" . ", 31),  // daily
    fillArray(" . ", 24)   // hourly
];

function sidebar_tree_tile(mx_tree, html_el) {
    html_el.append('<div class="dev-tile-content">Tree Name</div>'
        + '<div class="dev-tile-content">' + mx_tree.tree_name + '</div>'
        + '<div class="dev-tile-content">Dependent On</div>'
        + '<div class="dev-tile-content">' + mx_tree.dependent_on + '</div>'
        + '<div class="dev-tile-content">Dependant Trees</div>'
        + '<div class="dev-tile-content">' + mx_tree.dependant_trees + '</div>');
}

function sidebar_process_tile(process_entry, html_el) {
    html_el.append('<div class="dev-tile-content">Process Name</div>'
        + '<div class="dev-tile-content">' + process_entry.process_name + '</div>'
        + '<div class="dev-tile-content">Time Qualifier</div>'
        + '<div class="dev-tile-content">' + process_entry.time_qualifier + '</div>'
        + '<div class="dev-tile-content">State Machine</div>'
        + '<div class="dev-tile-content">' + process_entry.state_machine + '</div>'
        + '<div class="dev-tile-content">Blocking type</div>'
        + '<div class="dev-tile-content">' + process_entry.blocking_type + '</div>'
        + '<div class="dev-tile-content">Run On Active Timeperiod</div>'
        + '<div class="dev-tile-content">' + process_entry.run_on_active_timeperiod + '</div>'
        + '<div class="dev-tile-content">Blocking type</div>'
        + '<div class="dev-tile-content">' + process_entry.blocking_type + '</div>'
        + '<div class="dev-tile-content">Trigger Frequency</div>'
        + '<input type="text" size="8" maxlength="32" name="interval" value="' + process_entry.trigger_frequency + '" />');

}

function header_process_tile(process_entry, html_el) {
    html_el.append('<div class="dev-tile-content">Trigger On/Alive</div>'
        + '<div class="dev-tile-content">' + process_entry.is_on + '/' + process_entry.is_alive + '</div>'
        + '<div class="dev-tile-content">Process Name</div>'
        + '<div class="dev-tile-content">' + process_entry.process_name + '</div>'
        + '<div class="dev-tile-content">Next Timeperiod</div>'
        + '<div class="dev-tile-content">' + process_entry.next_timeperiod + '</div>'
        + '<div class="dev-tile-content">Next Run In</div>'
        + '<div class="dev-tile-content">' + process_entry.next_run_in + '</div>'
        + '<div class="dev-tile-content">' + 'Trigger Now Button' + '</div>'
        + '<div class="dev-tile-content">Reprocessing Queue</div>'
        + '<div class="dev-tile-content">' + row.next_timeperiod + '</div>');
}


$(function () {
    var mx_trees = [1, 2, 3, 4];
    var i;
    for (i in mx_trees) {
        var x = mx_trees[i];

        var grid_name = "column_" + x + "-grid";
        var el = document.getElementById(grid_name);
        var grid = new Tiles.Grid(el);

        // template is selected by user, not generated so just
        // return the number of columns in the current template
        grid.resizeColumns = function () {
            return this.template.numCols;
        };

        // by default, each tile is an empty div, we'll override creation
        // to add a tile number to each div
        grid.createTile = function (tileId) {
            var tile_process_name = "my_process_name";
            var tile_timeperiod = "2015010100";
            var tile_state = "state_processed";
            var tile_num_failed = "10";

            var checkbox_value = "{ process_name: '" + tile_process_name + "', timeperiod: '" + tile_timeperiod + "' }";
            var checkbox_div = '<input type="checkbox" name="batch_processing" value="' + checkbox_value + '"/>';

            var uow_button = $('<button>Get&nbsp;Uow</button>').click(function (e) {
                var params = { action: 'action_get_uow', timeperiod: tile_timeperiod, process_name: tile_process_name };
                var viewer_url = '/object_viewer/?' + $.param(params);
                window.open(viewer_url, 'Object Viewer', 'width=400,height=350,screenX=400,screenY=200,scrollbars=1');
            });
            var log_button = $('<button>View&nbsp;Log</button>').click(function (e) {
                var params = { action: 'action_get_log', timeperiod: tile_timeperiod, process_name: tile_process_name };
                var viewer_url = '/object_viewer/?' + $.param(params);
                window.open(viewer_url, 'Object Viewer', 'width=720,height=480,screenX=400,screenY=200,scrollbars=1');
            });

            var tile = new Tiles.Tile(tileId);
            tile.$el.attr('class', tile_state);
            tile.$el.append(checkbox_div);
            tile.$el.append(
                    '<div class="dev-tile-content">column id:' + i + '</div>'
                    + '<div class="dev-tile-content">tile id:' + tileId + '</div>'
                    + '<div class="dev-title-content">timeperiod:' + tile_timeperiod + '</div>'
                    + '<div class="dev-title-content">state:' + tile_state + '</div>'
                    + '<div class="dev-title-content">#fails:' + tile_num_failed + '</div>'
            );
            tile.$el.append($('<div></div>').append(uow_button));
            tile.$el.append($('<div></div>').append(log_button));
            return tile;
        };

        // get appropriate template
        var rows = DemoTemplateRows[i];

        // set the new template and resize the grid
        grid.template = Tiles.Template.fromJSON(rows);
        grid.isDirty = true;
        grid.resize();

        // adjust number of tiles to match selected template
        var ids = TILE_IDS.slice(0, grid.template.rects.length);
        grid.updateTiles(ids);
        grid.redraw(true);
    }

    // wait until users finishes resizing the browser
    var debouncedResize = debounce(function () {
        grid.resize();
        grid.redraw(true);
    }, 200);

    // when the window resizes, redraw the grid
    $(window).resize(debouncedResize);
});
