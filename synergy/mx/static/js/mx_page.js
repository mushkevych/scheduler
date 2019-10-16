/* @author "Bohdan Mushkevych" */

const GRIDS = {};

const GRID_HEADER_TEMPLATE = gridInfoTemplate(1);

function parseTimeperiod(t) {
    return t.slice(0, 4) + '-' + t.slice(4, 6) + '-' + t.slice(6, 8) + 'T' + t.slice(8);
}

/**
 * function returns a Tiles.js template for job records
 * template contains tiles_number of tiles
 * each tile has proportion 4x3 (wider than taller)
 */
function gridInfoTemplate(tiles_number) {
    const arr = [];
    for (let i = 0; i < tiles_number; i++) {
        if (i % 2 === 0) {
            // arr.push(" A A A A ");
            // arr.push(" A A A A ");
            // arr.push(" A A A A ");
            arr.push(' A ');
        } else {
            // arr.push(" B B B B ");
            // arr.push(" B B B B ");
            // arr.push(" B B B B ");
            arr.push(' B ');
        }
    }
    return arr;
}

function getGrid(grid_name) {
    let grid;
    let el;
    if (grid_name in GRIDS) {
        grid = GRIDS[grid_name];
    } else {
        el = document.getElementById(grid_name);
        grid = new Tiles.Grid(el);
        GRIDS[grid_name] = grid;
    }
    return grid;
}

function headerTreeTile(mx_tree, tile) {
    const refresh_button = $(`<button class="action_button auto-width mt-1" id="refresh_button_${mx_tree.tree_name}">
			<i class="fa fa-refresh"></i><span>
				Refresh
			</span></button>`).click(function (e) {
        clearTree(mx_tree.tree_name);
        mx_trees[mx_tree.tree_name] = getTree(mx_tree.tree_name);
        buildTree(mx_tree.tree_name);
    });
    let treeHeader = `
        <ul class="fa-ul header-tile-info process-info">
            <li title="Tree Name"><i class="fa-li fa fa-pagelines"></i>${mx_tree.tree_name}</li>
            <li title="Dependent On"><i class="fa-li fa fa-sitemap fa-rotate-180"></i>${formatJSON(mx_tree.dependent_on)}</li>
            <li title="Dependant Trees"><i class="fa-li fa fa-sitemap"></i>${formatJSON(mx_tree.dependant_trees)}</li>
        </ul>`;
    tile.$el.append(treeHeader);
    tile.$el.append(refresh_button);
    tile.$el.attr('class', 'tree_header_tile');
}

// xmlhttp object is declared in the .send() come from?
function headerProcessTile(process_entry, tile) {
    const flush_one_form = `
        <form class="process-form inline" method="GET" action="/gc/flush/one/" onsubmit="xmlhttp.send(); return false;">
            <input type="hidden" name="process_name" value=${process_entry.process_name} />
            <button type="submit" alt="Flush" title="flush_${process_entry.process_name}" class="inline fa"><i class="fa fa-recycle"></i>
            </button>
        </form>`;

    const reprocessing_block = `
        <div class="table_layout">
            <div class="inline table_layout_element"> ${flush_one_form} </div>
				<span class="inline table_layout_element">
				${process_entry.reprocessing_queue.toString() || '<em>--</em>'}
				</span>
				</div>`;

    const trigger_form = `
        <form class="process-form inline" method="POST" action="/managed/entry/trigger/" onsubmit="xmlhttp.send(); return false;">
			<input type="hidden" name="process_name" value="${process_entry.process_name}" />
			<input type="hidden" name="timeperiod" value="NA" />
			<button class="inline fa" type="submit" value="&#f135" alt="trigger_${process_entry.process_name}"><i class="fa fa-bolt"></i></button>
		</form>`;

    const next_run_block = `
        <div class="table_layout">
            <div class="inline table_layout_element"> ${trigger_form} </div>
            <span class="inline table_layout_element">
            ${process_entry.next_run_in || '<em>--</em>'}
            </span>
        </div>`;

    let is_on;
    // TODO: string interpolation
    if (process_entry.is_on) {
        is_on =
            "<a onclick=\"processTrigger('managed/entry/deactivate', '" + process_entry.process_name + "', 'NA', null, true)\">" +
            '<i class="fa fa-toggle-on action_toogle" title="is ON"></i></a>';
    } else {
        is_on =
            "<a onclick=\"processTrigger('managed/entry/activate', '" + process_entry.process_name + "', 'NA', null, true)\">" +
            '<i class="fa fa-toggle-off action_toogle" title="is OFF"></i></a>';
    }

    if (process_entry.is_alive) {
        tile.$el.attr('class', 'process_header_tile_on');
    } else {
        tile.$el.attr('class', 'process_header_tile_off');
    }

    let stateWarning =
        process_entry.is_on !== process_entry.is_alive
            ? '<i class="fa fa-exclamation state-warning" title="State inconsistent" aria-label="State inconsistent"></i>'
            : '';
    tile.$el.append(`
		<div class="header-tile-icons">
			<span class="fa" title="Trigger On/Off">
				<i class="fa fa-power-off"></i>
				${is_on}
			</span>
			<span class="fa">
				${stateWarning}
			</span>
        </div>
		<ul class="fa-ul process-info">
			<li title="Process Name"><i class="fa-li fa fa-terminal"></i>
				${process_entry.process_name}
			</li>
			<li title="Next Timeperiod"><i class="fa-li fa fa-play"></i>
				${parseTimeperiod(process_entry.next_timeperiod)}
			</li>
		</ul>
		${next_run_block}
		${reprocessing_block}
		`);
}

function infoProcessTile(process_entry, tile) {
    const change_interval_form = `
        <form class="process-form" method="POST" action="/managed/entry/interval/" onsubmit="xmlhttp.send(); return false;">
            <input type="hidden" name="process_name" value="${process_entry.process_name}" />
            <input type="hidden" name="timeperiod" value="NA" />
            <input type="text" size="8" maxlength="32" name="interval" value="${process_entry.trigger_frequency}" />
            <button type="submit" title="Apply" class="auto-width fa"><i class="fa fa-check"></i></button>
		</form>`;

    tile.process_name = process_entry.process_name;
    tile.$el.append(
        `<ul class="header-tile-info fa-ul"><li title="Process Name"><i class="fa-li fa fa-terminal"></i>
			<span class="justify">${process_entry.process_name}</span>
			</li><li title="Time Qualifier"><i class="fa-li fa fa-calendar"></i>
			<span class="justify">${process_entry.time_qualifier}</span>
			</li><li title="Time Grouping"><i class="fa-li fa fa-cubes"></i>
			<span class="justify">${process_entry.time_grouping}</span>
			</li><li title="State Machine"><i class="fa-li fa fa-puzzle-piece"></i>
			<span class="justify">${process_entry.state_machine_name}</span>
			</li><li title="Blocking Type"><i class="fa-li fa fa-anchor"></i>
			<span class="justify">${process_entry.blocking_type}</span>
			</li><li title="Trigger Frequency"><i class="fa-li fa fa-heartbeat"></i>
			<span class="justify">${change_interval_form}</span>
			</li>
        </ul>`
    );
    tile.$el.attr('class', 'process_info_tile');
}

function infoJobTile(job_entry, tile, is_next_timeperiod, is_selected_timeperiod) {
    const checkbox_value =
        "{ process_name: '" + job_entry.process_name + "', timeperiod: '" + job_entry.timeperiod + "' }";
    const checkbox_div = `
		<div class="checkbox">
			<input type="checkbox" name="batch_processing" value="${checkbox_value}" />
			<label></label>
		</div>
		`;
    const uow_button = $(
        `<button title="Uow" aria-lable="Uow" class="action_button">
			<i class="fa fa-file-code-o"></i>
			<span> Uow </span>
		</button>`
    ).click(function (e) {
        const params = {
            action: 'managed/uow',
            timeperiod: job_entry.timeperiod,
            process_name: job_entry.process_name
        };
        const viewer_url = '/viewer/object/?' + $.param(params);
        window.open(
            viewer_url,
            'Object Viewer',
            `width=${window.innerWidth * 2 / 3},height=${window.innerHeight * 0.85},scrollbars=1`
        );
    });
    const uow_log_button = $(
        `<button title="Uow Log" aria-lable="Uow Log" class="action_button">
			<i class="fa fa-file-text-o"></i>
			<span> Uow Log </span>
		</button>`
    ).click(function (e) {
        const params = {
            action: 'managed/log/uow',
            timeperiod: job_entry.timeperiod,
            process_name: job_entry.process_name
        };
        const viewer_url = '/viewer/object/?' + $.param(params);
        // HACK: see web.jpng.info
        window.open(
            viewer_url,
            'Object Viewer',
            `width=${window.innerWidth * 2 / 3},height=${window.innerHeight * 0.85},scrollbars=1`
        );
    });
    const event_log_button = $(
        `<button title="Event Log" aria-lable="Event Log" class="action_button">
			<i class="fa fa-th-list"></i>
			<span> Event Log </span>
		</button>`
    ).click(function (e) {
        const params = {
            action: 'managed/log/event',
            timeperiod: job_entry.timeperiod,
            process_name: job_entry.process_name
        };
        const viewer_url = '/viewer/object/?' + $.param(params);
        window.open(
            viewer_url,
            'Object Viewer',
            `width=${window.innerWidth * 2 / 3},height=${window.innerHeight * 0.85},scrollbars=1`
        );
    });
    const skip_button = $(
        `<button title="Skip" aria-lable="Skip" class="action_button">
			<i class="fa fa-step-forward"></i>
			<span> Skip </span>
		</button>`
    ).click(function (e) {
        processJob('tree/node/skip', tile.tree_name, tile.process_name, tile.timeperiod, null, null);
    });
    const reprocess_button = $(
        `<button title="Reprocess" aria-lable="Reprocess" class="action_button">
		    <i class="fa fa-repeat"></i>
			<span> Reprocess </span>
		</button>`
    ).click(function (e) {
        processJob('tree/node/reprocess', tile.tree_name, tile.process_name, tile.timeperiod, null, null);
    });
    const flow_button = $(
        `<button title="Workflow" aria-lable="Workflow" class="action_button">
			<i class="fa fa-random"></i>
			<span> Workflow </span>
		</button>`
    ).click(function (e) {
        const params = {
            action: 'flow/flow/details',
            timeperiod: job_entry.timeperiod,
            process_name: job_entry.process_name,
            unit_of_work_type: 'type_managed'
        };
        // FIXME: the window that is opened has a dependency for /static/jquery-3.1.1.min.js
        const viewer_url = '/viewer/flow/?' + $.param(params);
        window.open(
            viewer_url,
            'Flow Viewer',
            `width=${window.innerWidth * 2 / 3},height=${window.innerHeight * 0.85},scrollbars=1`
        );
    });

    tile.process_name = job_entry.process_name;
    tile.timeperiod = job_entry.timeperiod;
    tile.$el.click(function (e) {
        selectTile(tile);
    });

    if (is_next_timeperiod) {
        tile.$el.attr('class', job_entry.state + ' is_next_timeperiod');
    } else {
        tile.$el.attr('class', job_entry.state);
    }

    if (is_selected_timeperiod) {
        tile.$el.attr('class', job_entry.state + ' is_selected_timeperiod');
    }

    tile.$el.append(
        $(`<div class="tile_component">
				<ul class="fa-ul process-info">
					<li title="Timeperiod"><i class="fa-li fa fa-clock-o"></i> ${parseTimeperiod(job_entry.timeperiod)} </li>
					<li title="State"><i class="fa-li fa fa-flag-o"></i> ${job_entry.state} </li>
					<li title="# of fails"><i class="fa-li fa fa-exclamation-triangle"></i>
					${job_entry.number_of_failures}
					</li>
				</ul>
				${checkbox_div}
			</div>`)
    );
    tile.$el.append('<div class="clear"></div>');
    tile.$el.append(
        $('<div class="btn-container"></div>')
            .append(uow_button)
            .append(skip_button)
            .append(uow_log_button)
            .append(reprocess_button)
            .append(event_log_button)
            .append(flow_button)
    );
}

function buildHeaderGrid(grid_name, grid_template, builder_function, info_obj) {
    const grid = getGrid(grid_name);

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        const tile = new Tiles.Tile(tileId);
        builder_function(info_obj, tile);
        return tile;
    };

    // common post-build function calls per grid
    gridPostConstructor(grid, grid_template);
}

function buildProcessGrid(grid_name, tree_obj) {
    const grid = getGrid(grid_name);

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        const tile = new Tiles.Tile(tileId);

        // retrieve process entry
        const process_name = tree_obj.sorted_process_names[tileId - 1]; // tileId starts with 1
        const info_obj = tree_obj.processes[process_name];

        infoProcessTile(info_obj, tile);
        return tile;
    };

    // common post-build function calls per grid
    const template = gridInfoTemplate(tree_obj.sorted_process_names.length);
    gridPostConstructor(grid, template);
}

function buildJobGrid(grid_name, tree_level, next_timeperiod, selected_timeperiod, tree_obj) {
    const grid = getGrid(grid_name);
    const timeperiods = keysToList(tree_level.children, true);

    // set the tree for a grid
    grid.tree_obj = tree_obj;

    // by default, each tile is an empty div, we'll override creation
    // to add a tile number to each div
    grid.createTile = function (tileId) {
        const tile = new Tiles.Tile(tileId);
        tile.grid = grid;
        tile.tree_name = tree_obj.tree_name;

        // translate sequential IDs to the Timeperiods
        const reverse_index = timeperiods.length - tileId; // tileId starts with 1
        const timeperiod = timeperiods[reverse_index];

        // retrieve job_record
        const info_obj = tree_level.children[timeperiod];

        infoJobTile(info_obj, tile, next_timeperiod === timeperiod, selected_timeperiod === timeperiod);
        return tile;
    };

    // common post-build function calls per grid
    const template = gridInfoTemplate(timeperiods.length);
    gridPostConstructor(grid, template);
}

function gridPostConstructor(grid, template) {
    grid.template = Tiles.Template.fromJSON(template);

    // return the number of columns from original template
    // so that template does not change on the window resize
    grid.resizeColumns = function () {
        return this.template.numCols;
    };

    // adjust number of tiles to match selected template
    const ids = range(1, grid.template.rects.length);
    grid.updateTiles(ids);
    grid.resize();
    // HACK: turn of bad animation
    grid.redraw(false);

    // wait until users finishes resizing the browser
    const debounced_resize = debounce(function () {
        grid.resize();
        // FIXME: turn off bad animation
        grid.redraw(false);
    }, 200);

    // when the window resizes, redraw the grid
    $(window).resize(debounced_resize);
}

function getTreeNodes(process_name, timeperiod) {
    const response_text = $.ajax({
        data: {process_name: process_name, timeperiod: timeperiod},
        dataType: 'json',
        type: 'GET',
        url: '/tree/nodes/',
        cache: false,
        async: false
    }).responseText;
    return JSON.parse(response_text);
}

function getTree(tree_name) {
    const response_text = $.ajax({
        data: {tree_name: tree_name},
        dataType: 'json',
        type: 'GET',
        url: '/tree/',
        cache: false,
        async: false
    }).responseText;
    return JSON.parse(response_text);
}

function selectTile(tile) {
    // step 1: check if the tile is already selected
    if (tile.$el.attr('class').indexOf('is_selected_timeperiod') > -1) {
        // this tile is already selected
        // yet, we let it proceed as this is analogue to the "refresh"
        // return;
    }

    // step 2: remove is_selected_timeperiod class from all tiles in the given grid
    let i;
    const tile_number = tile.grid.tiles.length;
    for (i = 0; i < tile_number; i++) {
        const i_tile = tile.grid.tiles[i];
        const new_class = i_tile.$el.attr('class').replace(' is_selected_timeperiod', '');
        i_tile.$el.attr('class', new_class);
    }

    // step 3: assign is_selected_timeperiod to the given tile
    tile.$el.attr('class', tile.$el.attr('class') + ' is_selected_timeperiod');

    // FIXME: it would be better to replace the content, not the DOM element
    // step 4: iterate over grids and rebuild them
    const tree_obj = tile.grid.tree_obj;
    const process_number = tree_obj.sorted_process_names.length;
    if (tile.process_name === tree_obj.sorted_process_names[process_number - 1]) {
        // this is bottom-level process. no grid rebuilding is possible
        return;
    }

    let higher_next_timeperiod = tile.timeperiod;
    let higher_process_name = tile.process_name;
    let is_beyond_cut_off = false;
    let selected_timeperiod = undefined;

    for (i = 0; i < process_number; i++) {
        const i_process_name = tree_obj.sorted_process_names[i];
        if (is_beyond_cut_off === false) {
            if (i_process_name === tile.process_name) {
                is_beyond_cut_off = true;
                continue;
            } else {
                continue;
            }
        }

        // fetch child nodes for higher_process_name
        const tree_level = getTreeNodes(higher_process_name, higher_next_timeperiod);
        let process_obj = tree_obj.processes[i_process_name];

        higher_process_name = i_process_name;
        if (process_obj.next_timeperiod in tree_level.children) {
            higher_next_timeperiod = process_obj.next_timeperiod;
        } else {
            selected_timeperiod = Math.max.apply(Math, keysToList(tree_level.children, true));
            higher_next_timeperiod = selected_timeperiod;
        }

        // refresh job tiles: empty the grid-info
        let grid_name = 'grid-info-' + i_process_name;
        clearGrid(grid_name);

        // refresh job tiles: reconstruct the grid-info
        buildJobGrid(grid_name, tree_level, process_obj.next_timeperiod, selected_timeperiod, tree_obj);

        // refresh process tiles: empty the grid-header
        grid_name = 'grid-header-' + i_process_name;
        clearGrid(grid_name);

        // refresh process tiles: reconstruct the grid-header
        process_obj = getTree(tree_obj.tree_name).processes[i_process_name];
        buildHeaderGrid(grid_name, GRID_HEADER_TEMPLATE, headerProcessTile, process_obj);
    }
}

function clearGrid(grid_name) {
    const grid = getGrid(grid_name);
    grid.removeTiles(range(1, grid.tiles.length));
}

function clearTree(tree_name) {
    const tree_obj = mx_trees[tree_name];
    const process_number = tree_obj.sorted_process_names.length;
    let process_name = null;

    for (let i = 0; i < process_number; i++) {
        process_name = tree_obj.sorted_process_names[i];
        clearGrid('grid-header-' + process_name);
        clearGrid('grid-info-' + process_name);
    }
}

function buildTree(tree_name) {
    let i;
    let process_obj = null;
    let process_name = null;
    let tree_level = null;

    const tree_obj = mx_trees[tree_name];
    const process_number = tree_obj.sorted_process_names.length;

    // *** HEADER ***
    buildHeaderGrid('grid-header-' + tree_obj.tree_name, GRID_HEADER_TEMPLATE, headerTreeTile, tree_obj);

    for (i = 0; i < process_number; i++) {
        process_name = tree_obj.sorted_process_names[i];
        process_obj = tree_obj.processes[process_name];
        buildHeaderGrid('grid-header-' + process_name, GRID_HEADER_TEMPLATE, headerProcessTile, process_obj);
    }

    // *** INFO ***
    buildProcessGrid('grid-info-' + tree_obj.tree_name, tree_obj);

    let higher_next_timeperiod = null;
    let higher_process_name = null;
    for (i = 0; i < process_number; i++) {
        process_name = tree_obj.sorted_process_names[i];

        if (i === 0) {
            // fetching top level of the tree
            tree_level = getTreeNodes(process_name, higher_next_timeperiod);
        } else {
            tree_level = getTreeNodes(higher_process_name, higher_next_timeperiod);
        }

        process_obj = tree_obj.processes[process_name];
        higher_process_name = process_name;
        higher_next_timeperiod = process_obj.next_timeperiod;

        buildJobGrid('grid-info-' + process_name, tree_level, process_obj.next_timeperiod, undefined, tree_obj);
    }
}

/**
 * main method for the MX PAGE script
 * iterates over the @mx_trees map and commands building of the MX trees
 * NOTICE: variable @mx_trees is in the format {tree_name: tree_obj}
 *         and is set in mx_page_tiles.html by the templating engine
 */
$(function () {
    // former $(document).ready(function () {...})
    for (let tree_name in mx_trees) {
        if (!mx_trees.hasOwnProperty(tree_name)) {
            continue;
        }

        buildTree(tree_name);
    }
});
