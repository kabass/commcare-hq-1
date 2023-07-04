hqDefine("cloudcare/js/form_entry/spec/main", [
    "hqwebapp/js/mocha",
], function (
    hqMocha
) {
    hqRequire([
        "cloudcare/js/form_entry/spec/case_list_pagination_spec",
        "cloudcare/js/form_entry/spec/integration_spec",
        "cloudcare/js/form_entry/spec/utils_spec",
    ], function () {
        hqMocha.run();
    });

    return 1;
});
