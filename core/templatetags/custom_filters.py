from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """Get item from dictionary by key"""
    return dictionary.get(key)

@register.filter
def contains(value, arg):
    """Check if value contains arg"""
    return arg in str(value).lower()

@register.filter
def div(value, arg):
    """Divide value by arg"""
    try:
        return float(value) / float(arg)
    except (ValueError, ZeroDivisionError):
        return 0

@register.filter
def mul(value, arg):
    """Multiply value by arg"""
    try:
        return float(value) * float(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def split(value, delimiter):
    """Split string by delimiter"""
    if not value:
        return []
    return str(value).split(delimiter)

@register.filter
def first(value):
    """Get first item from list"""
    if not value:
        return ""
    return value[0] if len(value) > 0 else ""

@register.filter
def last(value):
    """Get last item from list"""
    if not value:
        return ""
    return value[-1] if len(value) > 0 else ""

@register.filter
def replace(value, arg):
    """Replace occurrences of a string with another string"""
    if not value or not arg:
        return value
    try:
        old, new = arg.split(',', 1)
        return str(value).replace(old.strip(), new.strip())
    except (ValueError, AttributeError):
        return value

@register.filter
def humanize_field_name(value):
    """Convert field names to human-readable format"""
    if not value:
        return value
    # Replace underscores with spaces and title case
    return str(value).replace('_', ' ').title()