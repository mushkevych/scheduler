// document holds all Scheduler MX UI client
var OUTPUT_DOCUMENT = {};

// main method for Scheduler MX UI client
$(document).ready(function () {
    $.get('/request_verticals/', function (response) {
        var result = OUTPUT_DOCUMENT.build_navigational_panel(response);
        $('#navigation').append(result);
        $('.menu').initMenu();
    }, 'json');
});

// method returns empty table for timetable records (panel on the right)
OUTPUT_DOCUMENT.get_empty_table = function (table_id) {
    return $('<table style="width: 60%" class="one-column-emphasis" id="' + table_id + '">\
                    <thead>\
                        <tr class="oce-first">\
                            <th scope="col">Time Period</th>\
                            <th scope="col">Process</th>\
                            <th scope="col">#Fail Calls</th>\
                            <th scope="col">State</th>\
                            <th scope="col"></th>\
                            <th scope="col"></th>\
                            <th scope="col"></th>\
                            <th scope="col"></th>\
                        </tr>\
                    </thead>\
              </table>');
};

// small helper method to wrap text into <td> tags
OUTPUT_DOCUMENT._to_td = function (text) {
    return $('<td>' + text + '</td>');
};

// method constructs HTML Table row with data and controls presenting timetable record
OUTPUT_DOCUMENT.construct_table_row = function (k, v, handler) {
    var tr = $('<tr></tr>').addClass(v.state);
    var tda = {};
    var timestamp_a = {};

    if (v.number_of_children > 0) {
        timestamp_a = $('<a href="#">' + k + '</a>').click(handler);
        tda = $('<td></td>').append(timestamp_a);
    } else {
        tda = $('<td>' + k + '</td>');
    }

    var reprocess_button = $('<button>Reprocess</button>').click(function (e) {
        process_timeperiod('action_reprocess', v.process_name, v.timeperiod, true)

    });
    var skip_button = $('<button>Skip</button>').click(function (e) {
        process_timeperiod('action_skip', v.process_name, v.timeperiod, true)

    });

    var uow_button = $('<button>Get&nbsp;Uow</button>').click(function (e) {
        var params = { action: 'action_get_uow', timeperiod: v.timeperiod, process_name: v.process_name };
        var viewer_url = '/object_viewer/?' + $.param(params);
        window.open(viewer_url, 'Object Viewer', 'width=400,height=350,screenX=400,screenY=200,scrollbars=1');
    });
    var log_button = $('<button>View&nbsp;Log</button>').click(function (e) {
        var params = { action: 'action_get_log', timeperiod: v.timeperiod, process_name: v.process_name };
        var viewer_url = '/object_viewer/?' + $.param(params);
        window.open(viewer_url, 'Object Viewer', 'width=720,height=480,screenX=400,screenY=200,scrollbars=1');
    });
    var td_button_repr = $('<td></td>').append(reprocess_button);
    var td_button_skip = $('<td></td>').append(skip_button);
    var td_button_uow = $('<td></td>').append(uow_button);
    var td_button_log = $('<td></td>').append(log_button);
    return tr.append(tda)
        .append(OUTPUT_DOCUMENT._to_td(v.process_name))
        .append(OUTPUT_DOCUMENT._to_td(v.number_of_failed_calls))
        .append(OUTPUT_DOCUMENT._to_td(v.state))
        .append(td_button_repr)
        .append(td_button_skip)
        .append(td_button_uow)
        .append(td_button_log);
};

// recursive method builds timerecords table on the right
OUTPUT_DOCUMENT.build_timerecords_panel = function (children, is_linear) {
    var table_id = 'unknown_process';
    var tbody = $('<tbody></tbody>');
    $.each(children, function (k, v) {
        table_id = v.process_name

        var handler = function (e) {
            e.preventDefault();
            var params = { timeperiod: v.timeperiod, process_name: v.process_name };
            $.get('/request_children/', params, function (response) {
                $('#content').html(OUTPUT_DOCUMENT.build_timerecords_panel(response.children, is_linear));
                OUTPUT_DOCUMENT.build_timerecord_entry(k, v, handler);
            });
        };
        var tr = OUTPUT_DOCUMENT.construct_table_row(k, v, handler);
        tbody.append(tr);
    });

    var table = OUTPUT_DOCUMENT.get_empty_table(table_id);
    table.append(tbody);
    table.dataTable({"bPaginate": is_linear,
        "bSort": true,
        "iDisplayLength": 36,
        "bLengthChange": false,
        "aaSorting": [
            [ 0, "desc" ]
        ]
    });

    return table;
};

// method builds single timerecord entry
OUTPUT_DOCUMENT.build_timerecord_entry = function (k, v, handler) {
    var tr = OUTPUT_DOCUMENT.construct_table_row(k, v, handler);
    tr.addClass(v.time_qualifier);
    if (typeof $('#level').find('table').val() == 'undefined') {
        var table = OUTPUT_DOCUMENT.get_empty_table(v.process_name);
        var tbody = $('<tbody></tbody>');
        tbody.append(tr);
        table.append(tbody);
        $('#level').append(table);
    } else {
        var the_tr = $('#level').find('table tr.' + v.time_qualifier);
        if (typeof the_tr.val() == 'undefined') {
            $('#level').find('tbody').append(tr);
        } else {
            the_tr.nextAll().empty();
            the_tr.replaceWith(tr);
        }
    }
};


// method builds navigational panel on the left
OUTPUT_DOCUMENT.build_navigational_panel = function (vertical_json) {

    // phase 1: sort process names
    var key_array = [];
    $.each(vertical_json, function (k, v) {
        key_array.push(k);
    });
    key_array.sort();

    // phase 2: define function to build expandable panel per process
    function per_process(k, v) {
        var li = $('<li></li>');
        var a = $('<a href="#">' + k + '</a>').click(function (e) {
            e.preventDefault();
            var params = {};
            if (v.number_of_levels == 1) {
                params = { process_name: v.processes.linear };
            } else {
                params = { process_name: v.processes.yearly };
            }

            $.get('/request_children/', params, function (response) {
                $('#level').empty();
                $('#content').empty();
                if (response.children) {
                    var right_ul = OUTPUT_DOCUMENT.build_timerecords_panel(response.children, v.number_of_levels == 1);
                    $('#content').hide().html(right_ul).fadeIn('1500');
                } else {
                    $('#content').hide().html('No report to show at this moment.').fadeIn('1500');
                }
            });
        });

        li.append(a);
        var ul1 = $('<ul class="acitem"></ul>');
        var next_timeperiods_li = $('<li><span class="subtitle">Next TimePeriods</span></li>');
        ul1.append(next_timeperiods_li);
        $.each(v.next_timeperiods, function (k1, v1) {
            ul1.append('<li><span class="detail">' + k1 + ':' + v1 + '</span></li>');
        });

        var reprocessing_queues_li = $('<li><span class="subtitle">Reprocessing Queues</span></li>');
        ul1.append(reprocessing_queues_li);
        $.each(v.reprocessing_queues, function (k2, v2) {
            ul1.append('<li><span class="detail">' + k2 + ':' + v2 + '</span></li>');
        });
        li.append(ul1);
        ul.append(li);
    }

    // phase 3: for each process name build an expandable panel
    var ul = $('<ul class="menu"></ul>');
    for (var i = 0; i < key_array.length; i++) {
        var k = key_array[i];
        var v = vertical_json[k];
        per_process(k, v);
    }
    return ul;
};

// Select all checkboxes
function toggle(source) {
    checkboxes = document.getElementsByName('batch_processing');
    for (var i = 0, n = checkboxes.length; i < n; i++) {
        checkboxes[i].checked = source.checked;
    }
}

// Display right click menu
$(window).load(function () {
    if (document.getElementsByClassName('process-menu').addEventListener) {
        document.getElementsByClassName('process-menu').addEventListener('contextmenu', function(e) {
            alert('Hm... please check the Java Script code to see what this means');
            e.preventDefault();
        }, false);
    } else {
//        $('body').on('contextmenu', '.process-menu', function () {
        document.getElementById("ProcessingStatements").attachEvent('oncontextmenu', function () {
            var y = mouseY(event);
            var x = mouseX(event);
            document.getElementById('rmenu').style.top = y + 'px';
            document.getElementById('rmenu').style.left = x + 'px';
            document.getElementById('rmenu').className = 'show';

            window.event.returnValue = false;
        });
    }
});

// hide the right-click-menu if user clicked outside its boundaries
$(document).bind('click', function (event) {
    document.getElementById('rmenu').className = 'hide';
});


function mouseX(evt) {
    if (evt.pageX) {
        return evt.pageX;
    } else if (evt.clientX) {
        return evt.clientX +
            (document.documentElement.scrollLeft ? document.documentElement.scrollLeft : document.body.scrollLeft);
    } else {
        return null;
    }
}

function mouseY(evt) {
    if (evt.pageY) {
        return evt.pageY;
    } else if (evt.clientY) {
        return evt.clientY +
            (document.documentElement.scrollTop ? document.documentElement.scrollTop : document.body.scrollTop);
    } else {
        return null;
    }
}
// End right click menu


// Get all checked rows
function get_checked_boxes(checkbox_name) {
    var checkboxes = document.getElementsByName(checkbox_name);
    var selected_checkboxes = [];

    for (var i = 0; i < checkboxes.length; i++) {
        if (checkboxes[i].checked) {
            selected_checkboxes.push(checkboxes[i]);
        }
    }
    // Return the array if it is non-empty, or null
    return selected_checkboxes.length > 0 ? selected_checkboxes : null;
}

function process_batch(action) {
    var selected = get_checked_boxes('batch_processing');
    var msg = 'You are about to ' + action + ' all selected';
    var i;
    var process_name;
    var unit_name;
    var timeperiod;
    var is_freerun;

    if (confirm(msg)) {
        if (action.indexOf('skip') > -1 || action.indexOf('reprocess') > -1) {
            for (i = 0; i < selected.length; i++) {
                process_name = selected[i].value['process_name'];
                timeperiod = selected[i].value['timeperiod'];
                process_timeperiod(action, process_name, timeperiod, false);
            }
        } else if (action.indexOf('activate') > -1 || action.indexOf('deactivate') > -1) {
            for (i = 0; i < selected.length; i++) {
                process_name = selected[i].value['process_name'];
                unit_name = selected[i].value['unit_name'];
                is_freerun = 'X' in selected[i].value;
                process_trigger(action, process_name, null, unit_name, is_freerun, false);
            }
        } else {
            alert('Action ' + action + ' is not supported by current MX JS')
        }
    }
}

function process_timeperiod(action, process_name, timeperiod, show_confirmation_dialog) {
    if (show_confirmation_dialog) {
        var msg = 'You are about to ' + action + ' ' + timeperiod + ' for ' + process_name;
        if (confirm(msg)) {
            // fall thru
        } else {
            return;
        }
    }

    var params = { timeperiod: timeperiod, process_name: process_name };
    $.get(action, params, function (response) {
//        alert("response is " + response);
    });
}

function process_trigger(action, process_name, timeperiod, unit_name, is_freerun, show_confirmation_dialog) {
    if (show_confirmation_dialog) {
        var msg = 'You are about to ' + action + ' ' + timeperiod + ' for ' + process_name;
        if (confirm(msg)) {
            // fall thru
        } else {
            return;
        }
    }

    var params;
    if (is_freerun) {
        params = { timeperiod: timeperiod, process_name: process_name };
    } else {
        params = { timeperiod: timeperiod, unit_name: unit_name, is_freerun: true };
    }

    $.get(action, params, function (response) {
//        alert("response is " + response);
    });
}
