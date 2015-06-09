/* @author "Bohdan Mushkevych" */

// function iterates over all checkboxes with name "batch_processing"
// and marks/unmarks them accordingly to the state of the "source" checkbox
function toggle_all_checkboxes(source) {
    var checkboxes = document.getElementsByName('batch_processing');
    for (var i = 0, n = checkboxes.length; i < n; i++) {
        checkboxes[i].checked = source.checked;
    }
}

// event call-back for "window.load" event
$(window).load(assign_context_menu());

// function replaces standard right-mouse-click menu with the custom one
function assign_context_menu() {
    var els = document.getElementsByClassName('context-menu');
    if (els) {
        Array.prototype.forEach.call(els, function (el) {
            el.addEventListener('contextmenu', function (event) {
                var y = mouse_y(event) - 2;  // subtract 2px to position pointer within the menu frame
                var x = mouse_x(event) - 2;  // subtract 2px to position pointer within the menu frame
                document.getElementById('rmenu').style.top = y + 'px';
                document.getElementById('rmenu').style.left = x + 'px';
                document.getElementById('rmenu').className = 'context_menu_show';

                event.preventDefault();
                var evt = event || window.event;
                evt.returnValue = false;
            }, false);
        });
    }
}

// hide the right-click-menu if user clicked outside its boundaries
$(document).bind('click', function (event) {
    if (document.getElementById('rmenu')) {
        document.getElementById('rmenu').className = 'context_menu_hide';
    }
});

// function allows to identify X coordinate to rendering context menu
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

// function allows to identify Y coordinate to rendering context menu
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

// function iterates over all checkboxes in the document and selects checked ones
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

// function applies given "action" to all records with selected checkboxes
function process_batch(action, is_freerun) {
    var selected = get_checked_boxes('batch_processing');
    var msg = 'You are about to ' + action + ' all selected';
    var i;
    var process_name;
    var timeperiod;
    var entry_name;
    var json;

    Alertify.confirm(msg, function (e) {
        if (!e) {
            return;
        }

        if (action.indexOf('skip') > -1 || action.indexOf('reprocess') > -1) {
            for (i = 0; i < selected.length; i++) {
                json = eval("(" + selected[i].value + ")");
                process_name = json['process_name'];
                timeperiod = json['timeperiod'];
                process_job(action, process_name, timeperiod, false);
                selected[i].checked = false;
            }
            Alertify.log("refresh the tree view", null, 1500, null);
        } else if (action.indexOf('activate') > -1 || action.indexOf('deactivate') > -1) {
            if (is_freerun) {
                for (i = 0; i < selected.length; i++) {
                    json = eval("(" + selected[i].value + ")");
                    process_name = json['process_name'];
                    entry_name = json['entry_name'];
                    process_trigger(action, process_name, null, entry_name, is_freerun, i < selected.length - 1, false);
                    selected[i].checked = false;
                }
            } else {
                for (i = 0; i < selected.length; i++) {
                    json = eval("(" + selected[i].value + ")");
                    process_name = json['process_name'];
                    timeperiod = json['timeperiod'];
                    process_trigger(action, process_name, timeperiod, null, is_freerun, i < selected.length - 1, false);
                    selected[i].checked = false;
                }
            }
        } else {
            Alertify.error('Action ' + action + ' is not supported by Synergy Scheduler MX JavaScript library.');
        }
    })

}

// function applies given "action" to the job record identified by "process_name+timeperiod"
function process_job(action, process_name, timeperiod, show_confirmation_dialog) {
    /**
     * function do_the_call performs communication with the server and parses response
     */
    function do_the_call() {
        var params = {'process_name': process_name, 'timeperiod': timeperiod};
        $.get('/' + action + '/', params, function (response) {
            if (response !== undefined && response !== null) {
                Alertify.log("response: " + response.responseText, null, 1500, null);
            }
            if (show_confirmation_dialog) {
                Alertify.log("refresh the tree view", null, 1500, null);
            }
        });
    }

    if (show_confirmation_dialog) {
        var msg = 'You are about to ' + action + ' ' + timeperiod + ' for ' + process_name;
        Alertify.confirm(msg, function (e) {
            if (!e) {
                return;
            }
            do_the_call();
        });
    } else {
        do_the_call();
    }
}

// function applies given "action" to the SchedulerThreadHandler entry
function process_trigger(action, process_name, timeperiod, entry_name, is_freerun, reload_afterwards, show_confirmation_dialog) {
    /**
     * function do_the_call performs communication with the server and reloads browser page afterwards
     */
    function do_the_call() {
        var params;
        if (is_freerun) {
            params = {'process_name': process_name, 'entry_name': entry_name, 'is_freerun': is_freerun};
        } else {
            params = {'process_name': process_name, 'timeperiod': timeperiod, 'is_freerun': is_freerun};
        }

        $.get('/' + action + '/', params, function (response) {
            // once the response arrives - reload the page
            if (reload_afterwards) {
                location.reload(true);
            }
        });
    }

    if (show_confirmation_dialog) {
        var msg = 'You are about to ' + action + ' ' + timeperiod + ' for ' + process_name;
        Alertify.confirm(msg, function (e) {
            if (!e) {
                return;
            }
            do_the_call();
        });
    } else {
        do_the_call();
    }
}
