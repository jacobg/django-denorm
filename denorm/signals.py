
from django.dispatch import Signal

post_denorm = Signal(providing_args=['instance'])