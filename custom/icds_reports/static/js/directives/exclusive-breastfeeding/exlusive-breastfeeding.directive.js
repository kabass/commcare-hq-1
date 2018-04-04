/* global d3, _ */
var url = hqImport('hqwebapp/js/initial_page_data').reverse;

function ExclusiveBreasfeedingController($scope, $routeParams, $location, $filter, maternalChildService,
    locationsService, userLocationId, storageService, genders, haveAccessToAllLocations, baseControllersService) {
    baseControllersService.BaseController.call(this, $scope, $routeParams, $location, locationsService,
        userLocationId, storageService);
    var vm = this;

    var genderIndex = _.findIndex(genders, function (x) {
        return x.id === vm.filtersData.gender;
    });
    if (genderIndex !== -1) {
        vm.genderLabel = genders[genderIndex].name;
    }

    vm.label = "Exclusive Breastfeeding";
    vm.steps = {
        'map': {route: '/exclusive_breastfeeding/map', label: 'Map View'},
        'chart': {route: '/exclusive_breastfeeding/chart', label: 'Chart View'},
    };
    vm.data = {
        legendTitle: 'Percentage Children',
    };
    vm.filters = ['age'];
    vm.rightLegend = {
        info: '"Percentage of infants 0-6 months of age who are fed exclusively with breast milk. An infant is exclusively breastfed if they recieve only breastmilk with no additional food, liquids (even water) ensuring optimal nutrition and growth between 0 - 6 months"',
    };

    vm.templatePopup = function(loc, row) {
        var gender = genderIndex > 0 ? genders[genderIndex].name : '';
        var chosenFilters = gender ? ' (' + gender + ') ' : '';
        var children = row ? $filter('indiaNumbers')(row.children) : 'N/A';
        var all = row ? $filter('indiaNumbers')(row.all) : 'N/A';
        var percent = row ? d3.format('.2%')(row.children / (row.all || 1)) : 'N/A';
        return vm.createTemplatePopup(
            loc.properties.name,
            [{
                indicator_name: 'Total number of children between ages 0 - 6 months' + chosenFilters + ': ',
                indicator_value: all,
            },
            {
                indicator_name: 'Total number of children (0-6 months) exclusively breastfed in the given month' + chosenFilters + ': ',
                indicator_value: children,
            },
            {
                indicator_name: '% children (0-6 months) exclusively breastfed in the given month' + chosenFilters + ': ',
                indicator_value: percent,
            }]
        );
    };

    vm.loadData = function () {
        vm.setStepsMapLabel();
        var usePercentage = true;
        var forceYAxisFromZero = false;
        vm.myPromise = maternalChildService.getExclusiveBreastfeedingData(vm.step, vm.filtersData).then(
            vm.loadDataFromResponse(usePercentage, forceYAxisFromZero)
        );
    };

    vm.init = function() {
        var locationId = vm.filtersData.location_id || vm.userLocationId;
        if (!locationId || ["all", "null", "undefined"].indexOf(locationId) >= 0) {
            vm.loadData();
            vm.loaded = true;
            return;
        }
        locationsService.getLocation(locationId).then(function(location) {
            vm.location = location;
            vm.loadData();
            vm.loaded = true;
        });
    };

    vm.init();

    $scope.$on('filtersChange', function() {
        vm.loadData();
    });

    vm.getDisableIndex = function () {
        var i = -1;
        if (!haveAccessToAllLocations) {
            window.angular.forEach(vm.selectedLocations, function (key, value) {
                if (key !== null && key.location_id !== 'all' && !key.user_have_access) {
                    i = value;
                }
            });
        }
        return i;
    };

    vm.moveToLocation = function(loc, index) {
        if (loc === 'national') {
            $location.search('location_id', '');
            $location.search('selectedLocationLevel', -1);
            $location.search('location_name', '');
        } else {
            $location.search('location_id', loc.location_id);
            $location.search('selectedLocationLevel', index);
            $location.search('location_name', loc.name);
        }
    };

    vm.chartOptions = {
        chart: {
            type: 'lineChart',
            height: 450,
            margin : {
                top: 20,
                right: 60,
                bottom: 60,
                left: 80,
            },
            x: function(d){ return d.x; },
            y: function(d){ return d.y; },

            color: d3.scale.category10().range(),
            useInteractiveGuideline: true,
            clipVoronoi: false,
            tooltips: true,
            xAxis: {
                axisLabel: '',
                showMaxMin: true,
                tickFormat: function(d) {
                    return d3.time.format('%b %Y')(new Date(d));
                },
                tickValues: function() {
                    return vm.chartTicks;
                },
                axisLabelDistance: -100,
            },

            yAxis: {
                axisLabel: '',
                tickFormat: function(d){
                    return d3.format(".2%")(d);
                },
                axisLabelDistance: 20,
                forceY: [0],
            },
            callback: function(chart) {
                var tooltip = chart.interactiveLayer.tooltip;
                tooltip.contentGenerator(function (d) {
                    var dataInMonth = _.find(vm.chartData[0].values, function(num) { return num['x'] === d.value;});
                    return vm.tooltipContent(d3.time.format('%b %Y')(new Date(d.value)), dataInMonth);
                });
                return chart;
            },
        },
        caption: {
            enable: true,
            html: '<i class="fa fa-info-circle"></i> Percentage of infants 0-6 months of age who are fed exclusively with breast milk. \n' +
            '\n' +
            'An infant is exclusively breastfed if they recieve only breastmilk with no additional food, liquids (even water) ensuring optimal nutrition and growth between 0 - 6 months',
            css: {
                'text-align': 'center',
                'margin': '0 auto',
                'width': '900px',
            }
        },
    };

    vm.tooltipContent = function (monthName, dataInMonth) {
        return "<p><strong>" + monthName + "</strong></p><br/>"
            + "<div>Total number of children between ages 0 - 6 months: <strong>" + $filter('indiaNumbers')(dataInMonth.all) + "</strong></div>"
            + "<div>Total number of children (0-6 months) exclusively breastfed in the given month: <strong>" + $filter('indiaNumbers')(dataInMonth.in_month) + "</strong></div>"
            + "<div>% children (0-6 months) exclusively breastfed in the given month: <strong>" + d3.format('.2%')(dataInMonth.y) + "</strong></div>";
    };

    vm.resetAdditionalFilter = function() {
        vm.filtersData.gender = '';
        $location.search('gender', null);
    };

    vm.showAllLocations = function () {
        return vm.all_locations.length < 10;
    };
}

ExclusiveBreasfeedingController.$inject = ['$scope', '$routeParams', '$location', '$filter', 'maternalChildService', 'locationsService', 'userLocationId', 'storageService', 'genders', 'haveAccessToAllLocations', 'baseControllersService'];

window.angular.module('icdsApp').directive('exclusiveBreastfeeding', function() {
    return {
        restrict: 'E',
        templateUrl: url('icds-ng-template', 'map-chart'),
        bindToController: true,
        scope: {
            data: '=',
        },
        controller: ExclusiveBreasfeedingController,
        controllerAs: '$ctrl',
    };
});
