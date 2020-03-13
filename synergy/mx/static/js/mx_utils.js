/* @author "Bohdan Mushkevych" */

/**
 * original source: underscorejs.org
 */
function debounce(func, wait, immediate) {
    let timeout;
    return function () {
        const context = this, args = arguments;
        const later = function () {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        if (immediate && !timeout) func.apply(context, args);
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * original source: http://joncom.be/code/realtypeof/
 */
function realTypeOf(v) {
    if (typeof (v) == "object") {
        if (v === null) return "null";
        if (v.constructor === (new Array).constructor) return "array";
        if (v.constructor === (new Date).constructor) return "date";
        if (v.constructor === (new RegExp).constructor) return "regex";
        return "object";
    }
    return typeof (v);
}

/**
 * original source: http://joncom.be/code/realtypeof/
 */
function formatJSON(oData, sIndent) {
    if (arguments.length < 2) {
        sIndent = "";
    }
    const sIndentStyle = "    ";
    const sDataType = realTypeOf(oData);
    let iCount = 0;
    let sHTML;

    // open object
    if (sDataType === "array") {
        if (oData.length === 0) {
            return "[]";
        }
        sHTML = "[";
    } else {
        iCount = 0;
        $.each(oData, function () {
            iCount++;
        });
        if (iCount === 0) { // object is empty
            return "{}";
        }
        sHTML = "{";
    }

    // loop through items
    iCount = 0;
    $.each(oData, function (sKey, vValue) {
        if (iCount > 0) {
            sHTML += ",";
        }
        if (sDataType === "array") {
            sHTML += ("\n" + sIndent + sIndentStyle);
        } else {
            sHTML += ("\n" + sIndent + sIndentStyle + "\"" + sKey + "\"" + ": ");
        }

        // display relevant data type
        switch (realTypeOf(vValue)) {
            case "array":
            case "object":
                sHTML += formatJSON(vValue, (sIndent + sIndentStyle));
                break;
            case "boolean":
            case "number":
                sHTML += vValue.toString();
                break;
            case "null":
                sHTML += "null";
                break;
            case "string":
                sHTML += ("\"" + vValue + "\"");
                break;
            default:
                sHTML += ("TYPEOF: " + typeof (vValue));
        }

        // loop
        iCount++;
    });

    // close object
    if (sDataType === "array") {
        sHTML += ("\n" + sIndent + "]");
    } else {
        sHTML += ("\n" + sIndent + "}");
    }

    return sHTML;
}


function keysToList(dictionary, sorted) {
    const keys = [];
    for (let key in dictionary) {
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


/**
 * parses url string of the browser window, where this script is running
 * and returns value of the parameter identified as <sParam>
 * @param sParam value of the parameter to return
 * @returns null if no parameter with given name found or its value otherwise
 */
function getUrlParameter(sParam) {
    const sPageURL = window.location.search.substring(1);
    const sURLVariables = sPageURL.split('&');
    for (let i = 0; i < sURLVariables.length; i++) {
        const sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] === sParam) {
            return sParameterName[1];
        }
    }
    return null;
}

/**
 * verifies if the given form_name::flag_name is set to *true*
 * triggers form submit if the flag is *false*
 * and converts all tables with style *synergy-datatable* to JS DataTable
 */
function loadDataset(form_name, flag_name, table_sorting) {
    if ($(form_name).data(flag_name) === false) {
        $(function () {
            $(form_name).submit();
        });
    }

    // convert HTML table into JS dataTable
    $('.synergy-datatable').dataTable({
        "bPaginate": true,
        "bSort": true,
        "iDisplayLength": 36,
        "bLengthChange": false,
        "aaSorting": table_sorting
    });

}

/**
 * creates an array of integers between two numbers, inclusive
 * @param start first value, inclusive
 * @param end last value, inclusive
 * @returns {Array}
 */
function range(start, end) {
    const myArray = [];
    for (let i = start; i <= end; i += 1) {
        myArray.push(i);
    }
    return myArray;
}

function refreshWithDelay() {
    setTimeout(function () {
        window.location.reload();
    }, 250);
}

function dateToTimeperiod(datetime) {
    const timeperiod = [
        datetime.getUTCFullYear(),
        ('0' + (datetime.getUTCMonth() + 1)).slice(-2),
        ('0' + datetime.getUTCDate()).slice(-2),
        ('0' + datetime.getUTCHours()).slice(-2),
    ].join('');

    return timeperiod;
}

function openViewerWindow(url, name) {
    const features =
        `toolbar=no, scrollbars=yes, resizable=yes,` +
        `top=${window.innerHeight * 0.15}, left=${window.innerWidth * 0.15},` +
        `width=${window.innerWidth / 2}, height=${window.innerHeight * 0.85}`;
    window.open(url, name, features);
    return false;
}

// also used by the synergy_flow
function submitHtmlForm(htmlForm) {
    let xhr = new XMLHttpRequest();
    xhr.open(htmlForm.method, htmlForm.action);
    xhr.send(new FormData(htmlForm));
    return false;
}
