// document holds all Synergy Scheduler MX UI content
var OUTPUT_DOCUMENT = {};

// main method for Scheduler MX UI client
$(document).ready(function () {
    $.get('/request_verticals/', function (response) {
        var result = OUTPUT_DOCUMENT.build_navigational_panel(response);
        $('#navigation').append(result);    // appends to the HTML document panel to the right (timeperiods)
        $('.menu').initMenu();              // appends to the HTML document navigational menu to the left (tree names)
    }, 'json');
});

// method returns empty table for timeperiod records (panel on the right)
OUTPUT_DOCUMENT.get_empty_table = function (table_id) {
    var table_class = 'one-column-emphasis context-menu  synergy_datatable';

    return $('<table style="width: 60%" class="' + table_class + '" id="' + table_id + '">\
                    <thead>\
                        <tr class="oce-first">\
                            <th scope="col"></th>\
                            <th scope="col">Time Period</th>\
                            <th scope="col">Process</th>\
                            <th scope="col">#Fail Calls</th>\
                            <th scope="col">State</th>\
                            <th scope="col"></th>\
                            <th scope="col"></th>\
                        </tr>\
                    </thead>\
              </table>');
};

// helper method to wrap text into <td> tags
OUTPUT_DOCUMENT._to_td = function (text) {
    return $('<td>' + text + '</td>');
};

// method constructs HTML Table row with data and controls presenting timetable record
OUTPUT_DOCUMENT.construct_table_row = function (k, v, handler) {
    var tr = $('<tr></tr>').addClass(v.state);
    var tda = {};
    var timeperiod_ahref = {};

    if (v.number_of_children > 0) {
        timeperiod_ahref = $('<a href="#">' + k + '</a>').click(handler);
        tda = $('<td></td>').append(timeperiod_ahref);
    } else {
        tda = $('<td>' + k + '</td>');
    }

    var checkbox_value = "{ process_name: '" + v.process_name + "', timeperiod: '" + k + "' }";
    var checkbox_td = '<td> <input type="checkbox" name="batch_processing" value="' + checkbox_value + '"/></td>';

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

    var td_button_uow = $('<td></td>').append(uow_button);
    var td_button_log = $('<td></td>').append(log_button);
    return tr.append(checkbox_td)
        .append(tda)
        .append(OUTPUT_DOCUMENT._to_td(v.process_name))
        .append(OUTPUT_DOCUMENT._to_td(v.number_of_failed_calls))
        .append(OUTPUT_DOCUMENT._to_td(v.state))
        .append(td_button_uow)
        .append(td_button_log);
};

// recursive method builds timerecords table on the right
OUTPUT_DOCUMENT.build_timerecords_panel = function (children, enable_pagination) {
    var table_id = 'unknown_process';
    var tbody = $('<tbody></tbody>');
    $.each(children, function (k, v) {
        table_id = v.process_name;

        var handler = function (e) {
            e.preventDefault();
            var params = { timeperiod: v.timeperiod, process_name: v.process_name };
            $.get('/request_children/', params, function (response) {
                $('#content').html(OUTPUT_DOCUMENT.build_timerecords_panel(response.children, enable_pagination));
                OUTPUT_DOCUMENT.build_timerecord_entry(k, v, handler);
                assign_context_menu();  // assign context menu to underlying levels of the tree (monthly, daily, hourly)
            });
        };
        var tr = OUTPUT_DOCUMENT.construct_table_row(k, v, handler);
        tbody.append(tr);
    });

    var table = OUTPUT_DOCUMENT.get_empty_table(table_id);
    table.append(tbody);

    if (!enable_pagination) {
        // this is multi-level case tree
        // convert HTML table into JS dataTable
        table.dataTable({"bSort": true,
            "iDisplayLength": 36,
            "aaSorting": [
                [ 1, "desc" ]
            ]
        });
    }

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

// method builds panel with the timeperiods on the right
OUTPUT_DOCUMENT.build_navigational_panel = function (vertical_json) {

    // phase 1: sort process names
    var key_array = [];
    $.each(vertical_json, function (k, v) {
        key_array.push(k);
    });
    key_array.sort();

    // phase 2: define function to build expandable panel per process
    function per_process(k, v) {
        var enable_pagination = v.number_of_levels == 1;
        var li = $('<li></li>');
        var a = $('<a href="#">' + k + '</a>').click(function (e) {
            e.preventDefault();
            var params = {};
            if (enable_pagination) {
                params = { process_name: v.processes.linear };
            } else {
                params = { process_name: v.processes.yearly };
            }

            $.get('/request_children/', params, function (response) {
                $('#level').empty();
                $('#content').empty();
                if (response.children) {
                    // construct HTML table with the list of timeperiods
                    var right_ul = OUTPUT_DOCUMENT.build_timerecords_panel(response.children, enable_pagination);
                    $('#content').hide().html(right_ul).fadeIn('1500');

                    if (enable_pagination) {
                        // this is linear-case tree
                        // convert HTML table into JS dataTable
                        $('.synergy_datatable').dataTable({"bPaginate": true,
                            "bSort": true,
                            "iDisplayLength": 36,
                            "bLengthChange": false,
                            "aaSorting": [
                                [ 1, "desc" ]
                            ]
                        });

                        $('.dataTables_wrapper').width('65%');  // change dataTable container width
                    }
                    assign_context_menu();                  // assign context menu to the table with top-level timeperiods (yearly, linear)
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
