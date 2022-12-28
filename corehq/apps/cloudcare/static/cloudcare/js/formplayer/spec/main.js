hqDefine("cloudcare/js/formplayer/spec/main", [
    "hqwebapp/js/mocha",
], function (
    hqMocha
) {
    hqRequire([
        "cloudcare/js/formplayer/spec/hq_events_spec",
        "cloudcare/js/formplayer/spec/menu_list_test",
    ], function () {
        hqMocha.run();
    });

    return 1;
});
