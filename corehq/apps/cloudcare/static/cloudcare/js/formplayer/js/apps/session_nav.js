/*global FormplayerFrontend, Util */

FormplayerFrontend.module("SessionNavigate", function (SessionNavigate, FormplayerFrontend, Backbone, Marionette) {
    SessionNavigate.Router = Marionette.AppRouter.extend({
        appRoutes: {
            "apps": "listApps", // list all apps available to this user
            ":session": "listMenus",
        },
    });

    var API = {
        listApps: function () {
            FormplayerFrontend.request("clearForm");
            SessionNavigate.AppList.Controller.listApps();
        },
        selectApp: function (appId) {
            SessionNavigate.MenuList.Controller.selectMenu(appId);
        },
        listMenus: function () {
            FormplayerFrontend.request("clearForm");
            var currentFragment = Backbone.history.getFragment();
            var urlObject = Util.CloudcareUrl.fromJson(Util.encodedUrlToObject(currentFragment));
            var appId = urlObject.appId;
            var sessionId = urlObject.sessionId;
            var steps = urlObject.steps;
            var page = urlObject.page;
            var search = urlObject.search;
            SessionNavigate.MenuList.Controller.selectMenu(appId, sessionId, steps, page, search);
        },
        showDetail: function (model, index) {
            SessionNavigate.MenuList.Controller.showDetail(model, index);
        },
        listSessions: function () {
            SessionNavigate.SessionList.Controller.listSessions();
        },
        getIncompleteForm: function (sessionId) {
            FormplayerFrontend.request("getIncompleteForm", sessionId);
        },
        renderResponse: function (menuResponse) {
            var NextScreenCollection = Backbone.Collection.extend({});
            var nextScreenCollection;
            //TODO: clean up this hackiness
            if (menuResponse.commands) {
                nextScreenCollection = new NextScreenCollection(menuResponse.commands);
                nextScreenCollection.type = "commands";
            } else {
                nextScreenCollection = new NextScreenCollection(menuResponse.entities);
                nextScreenCollection.type = "entities";
            }
            nextScreenCollection.title = menuResponse.title;
            nextScreenCollection.locales = menuResponse.locales;
            nextScreenCollection.sessionId = menuResponse.menuSessionId;
            var currentFragment = Backbone.history.getFragment();
            var urlObject = Util.CloudcareUrl.fromJson(Util.encodedUrlToObject(currentFragment));
            urlObject.setSessionId(nextScreenCollection.sessionId);
            var encodedUrl = Util.objectToEncodedUrl(urlObject.toJson());
            FormplayerFrontend.navigate(encodedUrl);
            SessionNavigate.MenuList.Controller.showMenu(nextScreenCollection);
        },
    };

    FormplayerFrontend.on("apps:currentApp", function () {
        var urlObject = Util.currentUrlToObject();
        urlObject.clearExceptApp();
        Util.setUrlToObject(urlObject);
        API.selectApp(urlObject.appId);
    });

    FormplayerFrontend.on("apps:list", function () {
        FormplayerFrontend.navigate("/");
        API.listApps();
    });

    FormplayerFrontend.on("app:select", function (appId) {
        var urlObject = new Util.CloudcareUrl(appId);
        Util.setUrlToObject(urlObject);
        API.selectApp(appId);
    });

    FormplayerFrontend.on("menu:select", function (index) {
        var urlObject = Util.currentUrlToObject();
        urlObject.addStep(index);
        Util.setUrlToObject(urlObject);
        API.listMenus();
    });

    FormplayerFrontend.on("menu:paginate", function (page) {
        var urlObject = Util.currentUrlToObject();
        urlObject.setPage(page);
        Util.setUrlToObject(urlObject);
        API.listMenus();
    });

    FormplayerFrontend.on("menu:search", function (search) {
        var urlObject = Util.currentUrlToObject();
        urlObject.setSearch(search);
        Util.setUrlToObject(urlObject);
        API.listMenus();
    });


    FormplayerFrontend.on("menu:show:detail", function (model, index) {
        API.showDetail(model, index);
    });

    FormplayerFrontend.on("sessions", function () {
        API.listSessions();
    });

    FormplayerFrontend.on("getIncompleteForm", function (sessionId) {
        API.getIncompleteForm(sessionId);
    });

    FormplayerFrontend.on("renderResponse", function (menuResponse) {
        API.renderResponse(menuResponse);
    });

    SessionNavigate.on("start", function () {
        new SessionNavigate.Router({
            controller: API,
        });
    });

    FormplayerFrontend.on("breadcrumbSelect", function (index) {
        var currentFragment = Backbone.history.getFragment();
        var paramMap = Util.getSteps(currentFragment);
        var steps = paramMap.steps.slice(0, index);
        SessionNavigate.MenuList.Controller.selectMenu(paramMap.appId, steps);
    });


});