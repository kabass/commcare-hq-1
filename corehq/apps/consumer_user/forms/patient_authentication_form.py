from django import forms
from django.contrib.auth.forms import AuthenticationForm
from corehq.apps.domain.forms import NoAutocompleteMixin
from django.utils.translation import ugettext_lazy as _
from django.core.exceptions import ValidationError
from corehq.apps.consumer_user.models import ConsumerUser
from corehq.apps.consumer_user.models import ConsumerUserCaseRelationship
from corehq.apps.hqcase.utils import update_case


class PatientAuthenticationForm(NoAutocompleteMixin, AuthenticationForm):
    username = forms.EmailField(label=_("Email Address"), max_length=75,
                                widget=forms.TextInput(attrs={'class': 'form-control'}),
                                required=True)
    password = forms.CharField(label=_("Password"),
                               widget=forms.PasswordInput(attrs={'class': 'form-control'}),
                               required=True)

    def __init__(self, *args, **kwargs):
        self.invitation = kwargs.pop('invitation', None)
        super().__init__(*args, **kwargs)

    def clean(self):
        username = self.cleaned_data.get('username')
        if username is None:
            raise ValidationError('Please enter a valid email address.')

        password = self.cleaned_data.get('password')
        if not password:
            raise ValidationError("Please enter a password.")
        try:
            cleaned_data = super(PatientAuthenticationForm, self).clean()
            if self.invitation and not self.invitation.accepted:
                consumer_user, _ = ConsumerUser.objects.get_or_create(user=self.user_cache)
                self.invitation.accepted = True
                self.invitation.save()
                _ = ConsumerUserCaseRelationship.objects.create(case_id=self.invitation.case_id,
                                                                domain=self.invitation.domain,
                                                                consumer_user=consumer_user)
                update_case(self.invitation.domain,
                            self.invitation.case_id,
                            {'invitation_status': 'accepted'})
        except ValidationError:
            raise
        return cleaned_data
