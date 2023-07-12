hqDefine('es6!cloudcare/js/sentry', [
    'hqwebapp/js/initial_page_data',
    'es6!sentry/js/sentry.browser.7.28.0.min',
    'es6!sentry/js/sentry.captureconsole.7.28.0.min',   // needed for Sentry.Integrations.CaptureConsole
], function (
    initialPageData,
    Sentry
) {

    let initSentry = function () {
        const sentryConfig = initialPageData.get('sentry');

        if (sentryConfig.dsn) {
            Sentry.init({
                dsn: sentryConfig.dsn,
                environment: sentryConfig.environment,
                release: sentryConfig.release,
                initialScope: {
                    tags: { "domain": initialPageData.get('domain') },
                    user: { "username": initialPageData.get('username') },
                },
                // interim measure, ideally we would clear breadcrumbs between sessions
                maxBreadcrumbs: 50,
                integrations: [
                    new Sentry.Integrations.Breadcrumbs({
                        dom: false,
                        console: false,
                    }),
                    new Sentry.Integrations.CaptureConsole({
                        levels: ["error"]
                    }),
                ],
                tunnel: initialPageData.reverse('report_sentry_error'),
                autoSessionTracking: false
            });
        }
    };

    return {
        initSentry: initSentry,
        Sentry: Sentry,
    };
});
