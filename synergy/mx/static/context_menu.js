// Select all checkboxes
function toggle_all_checkboxes(source) {
    checkboxes = document.getElementsByName('batch_processing');
    for (var i = 0, n = checkboxes.length; i < n; i++) {
        checkboxes[i].checked = source.checked;
    }
}

$(window).load(function () {
    var els = document.getElementsByClassName('context-menu');
    if (els) {
        Array.prototype.forEach.call(els, function(el) {
            el.addEventListener('contextmenu', function(event) {
                var y = mouse_y(event);
                var x = mouse_x(event);
                document.getElementById('rmenu').style.top = y + 'px';
                document.getElementById('rmenu').style.left = x + 'px';
                document.getElementById('rmenu').className = 'context_menu_show';

                event.preventDefault();
                var evt = event || window.event;
                evt.returnValue = false;
            }, false);
        });
    }
});

// hide the right-click-menu if user clicked outside its boundaries
$(document).bind('click', function (event) {
    document.getElementById('rmenu').className = 'context_menu_hide';
});

function mouse_x(event) {
    if (event.pageX) {
        return event.pageX;
    } else if (event.clientX) {
        return event.clientX +
            (document.documentElement.scrollLeft ? document.documentElement.scrollLeft : document.body.scrollLeft);
    } else {
        return null;
    }
}

function mouse_y(event) {
    if (event.pageY) {
        return event.pageY;
    } else if (event.clientY) {
        return event.clientY +
            (document.documentElement.scrollTop ? document.documentElement.scrollTop : document.body.scrollTop);
    } else {
        return null;
    }
}

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
            alert('Action ' + action + ' is not yet supported by Synergy Scheduler MX JavaScript library.')
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
