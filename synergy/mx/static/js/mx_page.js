/* @author "Bohdan Mushkevych" */

var GRIDS = {};

var GRID_HEADER_TEMPLATE = [" . "];


/**
 * function returns a Tiles.js template for job records
 * template contains tiles_number of tiles
 * each tile has proportion 3x2 (wider than taller)
 */
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


function get_grid(grid_name) {
    var grid;
    var el;
    if (grid_name in GRIDS) {
        grid = GRIDS[grid_name];
    } else {
        el = document.getElementById(grid_name);
        grid = new Tiles.Grid(el);
        GRIDS[grid_name] = grid;
    }
    return grid;
}

function header_tree_tile(mx_tree, tile) {
    tile.$el.append('<ul class="fa-ul">'
        + '<li title="Tree Name"><i class="fa-li fa fa-sitemap"></i>' + mx_tree.tree_name + '</li>'
        + '<li title="Dependent On"><i class="fa-li fa fa-expand"></i>' + formatJSON(mx_tree.dependent_on) + '</li>'
        + '<li title="Dependant Trees"><i class="fa-li fa fa-compress"></i>' + formatJSON(mx_tree.dependant_trees) + '</li>'
        + '</ul>');
    tile.$el.attr('class', 'tree_header_tile');
}


function header_process_tile(process_entry, tile) {
    var trigger_button = $('<button class="action_button"><i class="fa fa-paper-plane-o"></i>&nbsp;Trigger</button>').click(function (e) {
        var params = { 'process_name': process_entry.process_name, 'timeperiod': 'NA' };
        $.get('/action/trigger_now/', params, function (response) {
            if (response !== undefined && response !== null) {
                Alertify.log("response: " + response.responseText, null, 1500, null);
            }
        });
    });

    var is_on;
    if (process_entry.is_on) {
        is_on = '<a onclick="process_trigger(\'action/deactivate_trigger\', \'' + process_entry.process_name + '\', \'NA\', null, false, true, false)">' +
        '<i class="fa fa-toggle-on action_toogle" title="is ON"></i></a>';
    } else {
        is_on = '<a onclick="process_trigger(\'action/activate_trigger\', \'' + process_entry.process_name + '\', \'NA\', null, false, true, false)">' +
        '<i class="fa fa-toggle-off action_toogle" title="is OFF"></i></a>';
    }

    var is_alive;
    if (process_entry.is_alive) {
        is_alive = '<i class="fa fa-toggle-on" title="is ON"></i>';
        tile.$el.attr('class', 'process_header_tile_on');
    } else {
        is_alive = '<i class="fa fa-toggle-off" title="is OFF"></i>';
        tile.$el.attr('class', 'process_header_tile_off');
    }

    tile.$el.append('<ul class="fa-ul">'
        + '<li title="Trigger On/Off"><i class="fa-li fa fa-power-off"></i>' + is_on + '</li>'
        + '<li title="Trigger Alive"><i class="fa-li fa fa-bolt"></i>' + is_alive + '</li>'
        + '<li title="Process Name"><i class="fa-li fa fa-terminal"></i>' + process_entry.process_name + '</li>'
        + '<li title="Next Timeperiod"><i class="fa-li fa fa-play"></i>' + process_entry.next_timeperiod + '</li>'
        + '<li title="Next Run In"><i class="fa-li fa fa-rocket"></i>' + process_entry.next_run_in + '</li>'
        + '<li title="Reprocessing Queue"><i class="fa-li fa fa-retweet"></i>'
            + '<textarea class="reprocessing_queues" rows="2" cols="26" readonly>'
            + process_entry.reprocessing_queue
            + '</textarea>'
        + '</li>'
        + '</ul>');

    tile.$el.append($('<div class="action_button_li"></div>').append(trigger_button));
}


function info_process_tile(process_entry, tile) {
    var run_on_active_timeperiod;
    if (process_entry.run_on_active_timeperiod) {
        run_on_active_timeperiod = '<i class="fa fa-toggle-on" title="is ON"></i>';
    } else {
        run_on_active_timeperiod = '<i class="fa fa-toggle-off" title="is OFF"></i>';
    }

    var change_interval_form = '<form method="GET" action="/action/change_interval" onsubmit="xmlhttp.send(); return false;">'
        + '<input type="hidden" name="process_name" value="' + process_entry.process_name + '" />'
        + '<input type="hidden" name="timeperiod" value="NA" />'
        + '<input type="text" size="8" maxlength="32" name="interval" value="' + process_entry.trigger_frequency + '" />'
        + '<input type="submit" title="Apply" class="fa-input" value="&#xf00c;"/>'
        + '</form>';

    tile.process_name = process_entry.process_name;
    tile.$el.append('<ul class="fa-ul">'
        + '<li title="Process Name"><i class="fa-li fa fa-terminal"></i>' + process_entry.process_name + '</li>'
        + '<li title="Time Qualifier"><i class="fa-li fa fa-calendar"></i>' + process_entry.time_qualifier + '</li>'
        + '<li title="Time Grouping"><i class="fa-li fa fa-cubes"></i>' + process_entry.time_grouping + '</li>'
        + '<li title="State Machine"><i class="fa-li fa fa-puzzle-piece"></i>' + process_entry.state_machine_name + '</li>'
        + '<li title="Blocking type"><i class="fa-li fa fa-anchor"></i>' + process_entry.blocking_type + '</li>'
        + '<li title="Run On Active Timeperiod"><i class="fa-li fa fa-unlock-alt"></i>' + run_on_active_timeperiod + '</li>'
        + '<li title="Trigger Frequency"><i class="fa-li fa fa-heartbeat"></i>' + change_interval_form + '</li>'
        + '</ul>');
    tile.$el.attr('class', 'process_info_tile');
}


function info_job_tile(job_entry, tile, is_next_timeperiod, is_selected_timeperiod) {
    var checkbox_value = "{ process_name: '" + job_entry.process_name + "', timeperiod: '" + job_entry.timeperiod + "' }";
    var checkbox_div = '<input type="checkbox" name="batch_processing" value="' + checkbox_value + '"/>';

    var uow_button = $('<button class="action_button"><i class="fa fa-file-code-o"></i>&nbsp;Uow</button>').click(function (e) {
        var params = { action: 'action/get_uow', timeperiod: job_entry.timeperiod, process_name: job_entry.process_name };
        var viewer_url = '/object_viewer/?' + $.param(params);
        window.open(viewer_url, 'Object Viewer', 'width=400,height=350,screenX=400,screenY=200,scrollbars=1');
    });
    var log_button = $('<button class="action_button"><i class="fa fa-th-list"></i>&nbsp;Log</button>').click(function (e) {
        var params = { action: 'action/get_log', timeperiod: job_entry.timeperiod, process_name: job_entry.process_name };
        var viewer_url = '/object_viewer/?' + $.param(params);
        window.open(viewer_url, 'Object Viewer', 'width=720,height=480,screenX=400,screenY=200,scrollbars=1');
    });
    var skip_button = $('<button class="action_button"><i class="fa fa-step-forward"></i>&nbsp;Skip</button>').click(function (e) {
        process_job('action/skip', tile.process_name, tile.timeperiod, true)
    });
    var reprocess_button = $('<button class="action_button"><i class="fa fa-repeat"></i>&nbsp;Reprocess</button>').click(function (e) {
        process_job('action/reprocess', tile.process_name, tile.timeperiod, true)
    });

    tile.process_name = job_entry.process_name;
    tile.timeperiod = job_entry.timeperiod;
    tile.$el.click(function (e) {
        tile_selected(tile);
    });

    if (is_next_timeperiod) {
        tile.$el.attr('class', job_entry.state + ' is_next_timeperiod');
    } else {
        tile.$el.attr('class', job_entry.state);
    }

    if (is_selected_timeperiod) {
        tile.$el.attr('class', job_entry.state + ' is_selected_timeperiod');
    }

    tile.$el.append($('<div class="tile_component"></div>').append(checkbox_div));
    tile.$el.append($('<div class="tile_component"></div>').append('<ul class="fa-ul">'
        + '<li title="Timeperiod"><i class="fa-li fa fa-clock-o"></i>' + job_entry.timeperiod + '</li>'
        + '<li title="State"><i class="fa-li fa fa-flag-o"></i>' + job_entry.state + '</li>'
        + '<li title="# of fails"><i class="fa-li fa fa-exclamation-triangle"></i>' + job_entry.number_of_failures + '</li>'
        + '</ul>'));
    tile.$el.append('<div class="clear"></div>');
    tile.$el.append($('<div></div>').append(uow_button).append(skip_button));
    tile.$el.append($('<div></div>').append(log_button).append(reprocess_button));
}


function build_header_grid(grid_name, grid_template, builder_function, info_obj) {
    var grid = get_grid(grid_name);

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
    var grid = get_grid(grid_name);

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        var tile = new Tiles.Tile(tileId);

        // retrieve process entry
        var process_name = tree_obj.sorted_process_names[tileId - 1];  // tileId starts with 1
        var info_obj = tree_obj.processes[process_name];

        info_process_tile(info_obj, tile);
        return tile;
    };

    // common post-build function calls per grid
    var template = grid_info_template(tree_obj.sorted_process_names.length);
    grid_post_constructor(grid, template);
}


function build_job_grid(grid_name, tree_level, next_timeperiod, selected_timeperiod, tree_obj) {
    var grid = get_grid(grid_name);
    var timeperiods = keys_to_list(tree_level.children, true);

    // set the tree for a grid
    grid.tree_obj = tree_obj;

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        var tile = new Tiles.Tile(tileId);
        tile.grid = grid;

        // translate sequential IDs to the Timeperiods
        var reverse_index = timeperiods.length - tileId;    // tileId starts with 1
        var timeperiod = timeperiods[reverse_index];

        // retrieve job_record
        var info_obj = tree_level.children[timeperiod];

        info_job_tile(info_obj, tile, next_timeperiod==timeperiod, selected_timeperiod==timeperiod);
        return tile;
    };

    // common post-build function calls per grid
    var template = grid_info_template(timeperiods.length);
    grid_post_constructor(grid, template);
}


function grid_post_constructor(grid, template) {
    grid.template = Tiles.Template.fromJSON(template);

    // return the number of columns from original template
    // so that template does not change on the window resize
    grid.resizeColumns = function () {
        return this.template.numCols;
    };

    // adjust number of tiles to match selected template
    var ids = range(1, grid.template.rects.length);
    grid.updateTiles(ids);
    grid.resize();
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
    var response_text = $.ajax({
        data: {'process_name': process_name, 'timeperiod': timeperiod},
        dataType: "json",
        type: "GET",
        url: '/details/tree_nodes/',
        cache: false,
        async: false
    }).responseText;
    return JSON.parse(response_text);
}


function tile_selected(tile) {
    // step 1: check if the tile is already selected
    if (tile.$el.attr('class').indexOf('is_selected_timeperiod') > -1) {
        // this tile is already selected
        // yet, we let it proceed as this is analogue to the "refresh"
        // return;
    }

    // step 2: remove is_selected_timeperiod class from all tiles in the given grid
    var i;
    var tile_number = tile.grid.tiles.length;
    for (i = 0; i < tile_number; i++) {
        var i_tile = tile.grid.tiles[i];
        var new_class = i_tile.$el.attr('class').replace(' is_selected_timeperiod', '');
        i_tile.$el.attr('class', new_class);
    }

    // step 3: assign is_selected_timeperiod to the given title
    tile.$el.attr('class', tile.$el.attr('class') + ' is_selected_timeperiod');

    // step 4: iterate over grids and rebuild them
    var tree_obj = tile.grid.tree_obj;
    var process_number = tree_obj.sorted_process_names.length;
    if (tile.process_name == tree_obj.sorted_process_names[process_number - 1]) {
        // this is bottom-level process. no grid rebuilding is possible
        return;
    }

    var higher_next_timeperiod = tile.timeperiod;
    var higher_process_name = tile.process_name;
    var is_beyond_cut_off = false;
    var selected_timeperiod = undefined;

    for (i = 0; i < process_number; i++) {
        var i_process_name = tree_obj.sorted_process_names[i];
        if (is_beyond_cut_off == false) {
            if (i_process_name == tile.process_name) {
                is_beyond_cut_off = true;
                continue;
            } else {
                continue;
            }
        }

        // fetch child nodes for higher_process_name
        var tree_level = get_tree_nodes(higher_process_name, higher_next_timeperiod);
        var process_obj = tree_obj.processes[i_process_name];

        higher_process_name = i_process_name;
        if (process_obj.next_timeperiod in tree_level.children) {
            higher_next_timeperiod = process_obj.next_timeperiod;
        } else {
            selected_timeperiod = Math.max.apply(Math, keys_to_list(tree_level.children, true));
            higher_next_timeperiod = selected_timeperiod;
        }

        // empty the grid
        var grid_name = 'grid-info-' + i_process_name;
        var grid = get_grid(grid_name);
        grid.removeTiles(range(1, grid.tiles.length));

        // reconstruct the grid
        build_job_grid(grid_name, tree_level, process_obj.next_timeperiod, selected_timeperiod, tree_obj);
    }
}


function build_trees(mx_trees) {
    for (var tree_name in mx_trees) {
        if (!mx_trees.hasOwnProperty(tree_name)) {
            continue;
        }

        var i;
        var process_obj = null;
        var process_name = null;
        var tree_level = null;

        var tree_obj = mx_trees[tree_name];
        var process_number = tree_obj.sorted_process_names.length;

        // *** HEADER ***
        build_header_grid("grid-header-" + tree_obj.tree_name, GRID_HEADER_TEMPLATE, header_tree_tile, tree_obj);

        for (i = 0; i < process_number; i++) {
            process_name = tree_obj.sorted_process_names[i];
            process_obj = tree_obj.processes[process_name];
            build_header_grid("grid-header-" + process_name, GRID_HEADER_TEMPLATE, header_process_tile, process_obj);
        }

        // *** INFO ***
        build_process_grid("grid-info-" + tree_obj.tree_name, tree_obj);

        var higher_next_timeperiod = null;
        var higher_process_name = null;
        for (i = 0; i < process_number; i++) {
            process_name = tree_obj.sorted_process_names[i];

            if (i == 0) {
                // fetching top level of the tree
                tree_level = get_tree_nodes(process_name, higher_next_timeperiod);
            } else {
                tree_level = get_tree_nodes(higher_process_name, higher_next_timeperiod);
            }

            process_obj = tree_obj.processes[process_name];
            higher_process_name = process_name;
            higher_next_timeperiod = process_obj.next_timeperiod;

            build_job_grid("grid-info-" + process_name, tree_level, process_obj.next_timeperiod, undefined, tree_obj);
        }
    }
}


// main method for the MX PAGE script
$(document).ready(function () {
    // variable mx_trees is set in mx_page_tiles.html by the templating engine
    build_trees(mx_trees);

//    $.get('/details/trees/', function (response) {
//        build_trees(response);
//    }, 'json');
});
