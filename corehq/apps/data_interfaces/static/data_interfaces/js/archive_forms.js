hqDefine("data_interfaces/js/archive_forms", function() {
    var managementSelector = '#form-management',
        allFormsButtonSelector = managementSelector + ' input[name="select_all"]',
        checkboxesSelector = managementSelector + ' input.xform-checkbox',
        indicatorSelector = '#count_indicator';

    function updateFormCounts() {
        var selectedCount = $('#form-management').find('input.xform-checkbox:checked').length;
        $(".selectedCount").text(selectedCount);
        enable_disable_button(selectedCount);
    }

    function enable_disable_button(count){
        if (count == 0) {
            $("#submitForms").prop('disabled', true);
        }
        else {
            $("#submitForms").prop('disabled', false);
        }
    }

    function selectNone() {
        $(managementSelector + ' input.xform-checkbox:checked').prop('checked', false).change();
        $(allFormsButtonSelector).prop('checked', false);
    }

    $(function() {
        // bindings for 'all' button
        $(document).on('click', managementSelector + ' a.select-visible', function() {
            $(allFormsButtonSelector).prop('checked', false);
            $(checkboxesSelector).prop('checked', true).change();
            return false;
        });

        // bindings for 'none' button
        $(document).on('click', managementSelector + ' a.select-none', function() {
            selectNone();
            return false;
        });

        // bindings for form checkboxes
        $(document).on('change', checkboxesSelector, function() {
            // updates text like '3 of 5 selected'
            updateFormCounts();
            $(indicatorSelector).show();
        });
        $(document).on('click', checkboxesSelector, function() {
            $(allFormsButtonSelector).prop('checked', false);
        });

        // bindings for 'Select all' checkboxes
        $(document).on('click', allFormsButtonSelector, function() {
            if (this.checked) {
                $(checkboxesSelector).prop('checked', true).change();
                $(indicatorSelector).hide();
                enable_disable_button(1);
            }
            else {
                $(indicatorSelector).show();
                $(".selectedCount").text(0);
                $(managementSelector + ' a.select-none').click();
                enable_disable_button(0);
            }
        });

        // clear checkboxes when changing page
        $(document).on('mouseup', managementSelector + ' .dataTables_paginate a', selectNone);
        $(document).on('change', managementSelector + ' .dataTables_length select', selectNone);

        $(document).on('click', '#submitForms', function() {
            if ($(allFormsButtonSelector)[0].checked) {
                hqImport('analytix/js/google').track.event('Bulk Archive', 'All', 'Checkbox');
            } else {
                hqImport('analytix/js/google').track.event('Bulk Archive', 'All', 'Selected Forms');
            }
        })
    });
});
