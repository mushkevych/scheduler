var test_mx_trees = {
    'TreeSite': {
        'tree_name': 'TreeSite',
        'processes': {
            'SiteYearly': {
                'process_name': 'SiteYearly',
                'time_qualifier': '_yearly',
                'state_machine_name': 'discrete',
                'process_type': 'type_managed',
                'run_on_active_timeperiod': false,
                'reprocessing_queue': [],
                'next_timeperiod': '2015000000',
                'trigger_frequency': 'every 14000',
                'state': 'state_on',
                'blocking_type': 'blocking_normal'
            },
            'SiteMonthly': {
                'process_name': 'SiteMonthly',
                'time_qualifier': '_monthly',
                'state_machine_name': 'discrete',
                'process_type': 'type_managed',
                'run_on_active_timeperiod': false,
                'reprocessing_queue': [],
                'next_timeperiod': '2015030000',
                'trigger_frequency': 'every 7000',
                'state': 'state_on',
                'blocking_type': 'blocking_normal'
            },
            'SiteDaily': {
                'process_name': 'SiteDaily',
                'time_qualifier': '_daily',
                'state_machine_name': 'discrete',
                'process_type': 'type_managed',
                'run_on_active_timeperiod': false,
                'reprocessing_queue': [],
                'next_timeperiod': '2015030100',
                'trigger_frequency': 'every 3600',
                'state': 'state_on',
                'blocking_type': 'blocking_normal'
            },
            'SiteHourly': {
                'process_name': 'SiteHourly',
                'time_qualifier': '_hourly',
                'state_machine_name': 'discrete',
                'process_type': 'type_managed',
                'run_on_active_timeperiod': false,
                'reprocessing_queue': [],
                'next_timeperiod': '2015030101',
                'trigger_frequency': 'every 900',
                'state': 'state_on',
                'blocking_type': 'blocking_normal'
            }
        }
    },
    'TreeAlert': {
        'tree_name': 'TreeAlert',
        'processes': {
            'AlertDaily': {
                'process_name': 'AlertDaily',
                'time_qualifier': '_daily',
                'state_machine_name': 'discrete',
                'process_type': 'type_managed',
                'run_on_active_timeperiod': false,
                'reprocessing_queue': [],
                'next_timeperiod': '2015030100',
                'trigger_frequency': 'every 900',
                'state': 'state_on',
                'blocking_type': 'blocking_normal'
            }
        }
    }
};


// The grid manages tiles using ids, which you can define. For our
// examples we'll just use the tile number as the unique id.
var TILE_IDS = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31
];

var GridHeaderTemplate = [" . "];


// debounce utility from underscorejs.org
function debounce(func, wait, immediate) {
    var timeout;
    return function () {
        var context = this, args = arguments;
        var later = function () {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        if (immediate && !timeout) func.apply(context, args);
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}


function keys_to_list(dictionary, sorted) {
    var keys = [];
    for (var key in dictionary) {
        if (dictionary.hasOwnProperty(key)) {
            keys.push(key);
        }
    }

    if (sorted) {
        return keys.sort();
    } else {
        return keys;
    }
}


// function returns a Tiles.js template for job records
// template contains tiles_number of tiles
// each tile has proportion 3x2 (wider than taller)
function grid_info_template(tiles_number) {
    var arr = [];
    for (var i = 0; i < tiles_number; i++) {
        if (i % 2 == 0) {
            arr.push(" A A A ");
            arr.push(" A A A ");
        } else {
            arr.push(" B B B ");
            arr.push(" B B B ");
        }
    }
    return arr;
}


function header_tree_tile(mx_tree, tile) {
    tile.$el.append('<div class="dev-tile-content">Tree Name</div>'
        + '<div class="dev-tile-content">' + mx_tree.tree_name + '</div>'
        + '<div class="dev-tile-content">Dependent On</div>'
        + '<div class="dev-tile-content">' + mx_tree.dependent_on + '</div>'
        + '<div class="dev-tile-content">Dependant Trees</div>'
        + '<div class="dev-tile-content">' + mx_tree.dependant_trees + '</div>');
}


function header_process_tile(process_entry, tile) {
    tile.$el.append('<div class="dev-tile-content">Trigger On/Alive</div>'
        + '<div class="dev-tile-content">' + process_entry.is_on + '/' + process_entry.is_alive + '</div>'
        + '<div class="dev-tile-content">Process Name</div>'
        + '<div class="dev-tile-content">' + process_entry.process_name + '</div>'
        + '<div class="dev-tile-content">Next Timeperiod</div>'
        + '<div class="dev-tile-content">' + process_entry.next_timeperiod + '</div>'
        + '<div class="dev-tile-content">Next Run In</div>'
        + '<div class="dev-tile-content">' + process_entry.next_run_in + '</div>'
        + '<div class="dev-tile-content">' + 'Trigger Now Button' + '</div>'
        + '<div class="dev-tile-content">Reprocessing Queue</div>'
        + '<div class="dev-tile-content">' + process_entry.reprocessing_queue + '</div>');
}


function info_process_tile(process_entry, tile) {
    tile.process_name = process_entry.process_name;
    tile.$el.append('<div class="dev-tile-content">Process Name</div>'
        + '<div class="dev-tile-content">' + process_entry.process_name + '</div>'
        + '<div class="dev-tile-content">Time Qualifier</div>'
        + '<div class="dev-tile-content">' + process_entry.time_qualifier + '</div>'
        + '<div class="dev-tile-content">State Machine</div>'
        + '<div class="dev-tile-content">' + process_entry.state_machine_name + '</div>'
        + '<div class="dev-tile-content">Blocking type</div>'
        + '<div class="dev-tile-content">' + process_entry.blocking_type + '</div>'
        + '<div class="dev-tile-content">Run On Active Timeperiod</div>'
        + '<div class="dev-tile-content">' + process_entry.run_on_active_timeperiod + '</div>'
        + '<div class="dev-tile-content">Trigger Frequency</div>'
        + '<input type="text" size="8" maxlength="32" name="interval" value="' + process_entry.trigger_frequency + '" />');
}


function info_job_tile(job_entry, tile, is_next_timeperiod) {
    var checkbox_value = "{ process_name: '" + job_entry.process_name + "', timeperiod: '" + job_entry.timeperiod + "' }";
    var checkbox_div = '<input type="checkbox" name="batch_processing" value="' + checkbox_value + '"/>';

    var uow_button = $('<button>Get&nbsp;Uow</button>').click(function (e) {
        var params = { action: 'action_get_uow', timeperiod: job_entry.timeperiod, process_name: job_entry.process_name };
        var viewer_url = '/object_viewer/?' + $.param(params);
        window.open(viewer_url, 'Object Viewer', 'width=400,height=350,screenX=400,screenY=200,scrollbars=1');
    });
    var log_button = $('<button>View&nbsp;Log</button>').click(function (e) {
        var params = { action: 'action_get_log', timeperiod: job_entry.timeperiod, process_name: job_entry.process_name };
        var viewer_url = '/object_viewer/?' + $.param(params);
        window.open(viewer_url, 'Object Viewer', 'width=720,height=480,screenX=400,screenY=200,scrollbars=1');
    });

    tile.process_name = job_entry.process_name;
    tile.timeperiod = job_entry.timeperiod;

    tile.$el.attr('class', job_entry.state);
    if (is_next_timeperiod) {
        tile.$el.className += ' is_next_timeperiod';
    }

    tile.$el.append($('<div></div>').append(checkbox_div).append(' p/t: ' + job_entry.process_name + '/' + tile.id));
    tile.$el.append('<div class="dev-title-content">timeperiod: ' + job_entry.timeperiod + '</div>'
            + '<div class="dev-title-content">state: ' + job_entry.state + '</div>'
            + '<div class="dev-title-content">#fails: ' + job_entry.number_of_failures + '</div>'
    );
    tile.$el.append($('<div></div>').append(uow_button));
    tile.$el.append($('<div></div>').append(log_button));
}


function build_header_grid(grid_name, grid_template, builder_function, info_obj) {
    var el = document.getElementById(grid_name);
    var grid = new Tiles.Grid(el);

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        var tile = new Tiles.Tile(tileId);
        builder_function(info_obj, tile);
        return tile;
    };

    // common post-build function calls per grid
    grid_post_constructor(grid, grid_template);
}


function build_process_grid(grid_name, tree_obj) {
    var el = document.getElementById(grid_name);
    var grid = new Tiles.Grid(el);
    var process_names = keys_to_list(tree_obj.processes, true);

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        var tile = new Tiles.Tile(tileId);

        // retrieve process entry
        var process_name = process_names[tileId];
        var info_obj = tree_obj.processes[process_name];

        info_process_tile(info_obj, tile);
        return tile;
    };

    // common post-build function calls per grid
    var template = grid_info_template(process_names.length);
    grid_post_constructor(grid, template);
}


function build_job_grid(grid_name, tree_level, next_timeperiod) {
    var el = document.getElementById(grid_name);
    var grid = new Tiles.Grid(el);
    var timeperiods = keys_to_list(tree_level.children, true);

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        var tile = new Tiles.Tile(tileId);

        // translate sequential IDs to the Timeperiods
        var reverse_index = timeperiods.length - tileId;
        var timeperiod = timeperiods[reverse_index];

        // retrieve job_record
        var info_obj = tree_level.children[timeperiod];

        info_job_tile(info_obj, tile, next_timeperiod==timeperiod);
        return tile;
    };

    // common post-build function calls per grid
    var template = grid_info_template(timeperiods.length);
    grid_post_constructor(grid, template);
}


function grid_post_constructor(grid, template) {
    grid.template = Tiles.Template.fromJSON(template);
    grid.isDirty = true;
    grid.resize();

    // template is selected by user, not generated so just
    // return the number of columns in the current template
    grid.resizeColumns = function () {
        return this.template.numCols;
    };

    // adjust number of tiles to match selected template
    var ids = TILE_IDS.slice(0, grid.template.rects.length);
    grid.updateTiles(ids);
    grid.redraw(true);

    // wait until users finishes resizing the browser
    var debounced_resize = debounce(function () {
        grid.resize();
        grid.redraw(true);
    }, 200);

    // when the window resizes, redraw the grid
    $(window).resize(debounced_resize);
}


function get_tree_nodes(process_name, timeperiod){
    return $.ajax({
        data: {'process_name': process_name, 'timeperiod': timeperiod},
        dataType: "json",
        type: "GET",
        url: '/request_tree_nodes/',
        cache: false,
        async: false
    });
}


function build_trees(mx_trees) {
    for (var tree_name in mx_trees) {
        if (!mx_trees.hasOwnProperty(tree_name)) {
            continue;
        }

        var tree_obj = mx_trees[tree_name];
        var process_obj;
        var process_name;

        // *** HEADER ***
        build_header_grid("grid-header-" + tree_obj.tree_name, GridHeaderTemplate, header_tree_tile, tree_obj);

        for (process_name in tree_obj.processes) {
            if (!tree_obj.processes.hasOwnProperty(process_name)) {
                continue;
            }

            process_obj = tree_obj.processes[process_name];
            build_header_grid("grid-header-" + process_name, GridHeaderTemplate, header_process_tile, process_obj);
        }


        // *** INFO ***
        build_process_grid("grid-info-" + tree_obj.tree_name, tree_obj);

        var tree_level;
        var higher_next_timeperiod = null;
        var higher_process_name = null;
        for (process_name in tree_obj.processes) {
            if (!tree_obj.processes.hasOwnProperty(process_name)) {
                continue;
            }

            if (process_name == get_tree_top_process(tree_obj)) {
                // fetching top level of the tree
                tree_level = get_tree_nodes(process_name, higher_next_timeperiod);
            } else {
                tree_level = get_tree_nodes(higher_process_name, higher_next_timeperiod);
            }

            process_obj = tree_obj.processes[process_name];
            higher_process_name = process_name;
            higher_next_timeperiod = process_obj.next_timeperiod;

            build_job_grid("grid-info-" + process_name, tree_level, process_obj.next_timeperiod);
        }
    }
}


// main method for the MX PAGE script
$(document).ready(function () {
    $.get('/request_trees/', function (response) {
        build_trees(test_mx_trees);
//        build_trees(response);
    }, 'json');
});
