from django.utils.translation import gettext_lazy as _
from django.urls import reverse
from django.http import HttpResponseRedirect, HttpResponseBadRequest
from django.http import Http404

from corehq.apps.hqwebapp.views import CRUDPaginatedViewMixin
from corehq.apps.domain.views.base import BaseDomainView
from corehq.apps.events.models import Event
from corehq.apps.events.forms import CreateEventForm
from corehq.apps.hqwebapp.decorators import use_jquery_ui, use_multiselect
from corehq import toggles


class BaseEventView(BaseDomainView):
    urlname = "events_page"
    section_name = _("Attendance Tracking")

    def dispatch(self, *args, **kwargs):
        # The FF check is temporary till the full feature is released
        toggle_enabled = toggles.ATTENDANCE_TRACKING.enabled(self.domain)
        if not (self.request.couch_user.can_manage_events(self.domain) and toggle_enabled):
            raise Http404
        return super(BaseEventView, self).dispatch(*args, **kwargs)

    @property
    def section_url(self):
        return reverse(BaseEventView.urlname, args=(self.domain,))


class EventsView(BaseEventView, CRUDPaginatedViewMixin):
    urlname = "events_page"
    template_name = 'events_list.html'

    page_title = _("Attendance Tracking Events")

    limit_text = _("events per page")
    empty_notification = _("You have no events")
    loading_message = _("Loading events")

    @property
    def page_name(self):
        return _("Events")

    @property
    def page_url(self):
        return reverse(self.urlname, args=(self.domain,))

    @property
    def total(self):
        return self.domain_events.count()

    @property
    def column_names(self):
        return [
            _("Name"),
            _("Start date"),
            _("End date"),
            _("Attendance Target"),
            _("Status"),
            _("Total attendees"),
        ]

    @property
    def page_context(self):
        return self.pagination_context

    @property
    def domain_events(self):
        return Event.objects.by_domain(self.domain, most_recent_first=True)

    @property
    def paginated_list(self):
        start, end = self.skip, self.skip + self.limit
        for event in self.domain_events[start:end]:
            yield {
                'itemData': self._format_paginated_event(event),
                'template': 'base-event-template'
            }

    def post(self, *args, **kwargs):
        return self.paginate_crud_response

    def _format_paginated_event(self, event: Event):
        return {
            'id': event.event_id,
            'name': event.name,
            'start_date': str(event.start_date),
            'end_date': str(event.end_date),
            'target_attendance': event.attendance_target,
            'status': event.status,
            'total_attendance': event.total_attendance or '-',
            'edit_url': reverse(EventEditView.urlname, args=(self.domain, event.event_id)),
            'delete_url': reverse('remove_event', args=(self.domain, event.event_id)),
        }


class EventCreateView(BaseEventView):
    urlname = 'add_attendance_tracking_event'
    template_name = "new_event.html"

    page_title = _("Add Attendance Tracking Event")

    @use_multiselect
    @use_jquery_ui
    def dispatch(self, request, *args, **kwargs):
        return super(EventCreateView, self).dispatch(request, *args, **kwargs)

    @property
    def page_name(self):
        return _("Add New Event")

    @property
    def page_url(self):
        return reverse(self.urlname, args=(self.domain,))

    @property
    def parent_pages(self):
        return [
            {
                'title': _("Events"),
                'url': reverse(EventsView.urlname, args=[self.domain])
            },
        ]

    def get_context_data(self, **kwargs):
        context = super(EventCreateView, self).get_context_data(**kwargs)
        context.update({'form': self.form})
        return context

    def post(self, request, *args, **kwargs):
        form = self.form

        if not form.is_valid():
            return self.get(request, *args, **kwargs)

        event_data = form.cleaned_data

        event = Event(
            name=event_data['name'],
            domain=self.domain,
            start_date=event_data['start_date'],
            end_date=event_data['end_date'],
            attendance_target=event_data['attendance_target'],
            sameday_reg=event_data['sameday_reg'],
            track_each_day=event_data['track_each_day'],
            manager_id=self.request.couch_user.user_id,
        )
        event.save(expected_attendees=event_data['expected_attendees'])

        return HttpResponseRedirect(reverse(EventsView.urlname, args=(self.domain,)))

    @property
    def form(self):
        if self.request.method == 'POST':
            return CreateEventForm(self.request.POST, domain=self.domain)
        else:
            return CreateEventForm(initial={}, domain=self.domain)
            return CreateEventForm(event=self.event, domain=self.domain)

    @property
    def event(self):
        return None


class EventEditView(EventCreateView):
    urlname = 'edit_attendance_tracking_event'
    template_name = "new_event.html"
    http_method_names = ['get', 'post']

    page_title = _("Edit Attendance Tracking Event")
    event_obj = None

    @use_multiselect
    @use_jquery_ui
    def dispatch(self, request, *args, **kwargs):
        self.event_obj = Event.get_event_by_id(kwargs['event_id'])
        return super(EventCreateView, self).dispatch(request, *args, **kwargs)

    @property
    def page_name(self):
        return _("Edit Event")

    @property
    def page_url(self):
        return reverse(self.urlname, args=(self.domain, self.event.event_id))

    @property
    def event(self):
        if not self.event_obj:
            raise NoEventError
        return self.event_obj


def remove_event(request, *args, **kwargs):
    Event.delete_event(kwargs['event_id'], kwargs['domain'])
    return HttpResponseRedirect(reverse(EventsView.urlname, args=(kwargs['domain'],)))