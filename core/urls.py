from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('commercial/insights/', views.commercial_rate_insights, name='commercial_rate_insights'),
    path('commercial/insights/map/', views.commercial_rate_insights_map, name='commercial_rate_insights_map'),
    path('commercial/insights/<str:state_code>/overview/', views.commercial_rate_insights_overview, name='commercial_rate_insights_overview'),
    path('commercial/insights/<str:state_code>/overview-simple/', views.commercial_rate_insights_overview_simple, name='commercial_rate_insights_overview_simple'),
    path('commercial/insights/<str:state_code>/api/filter-options/', views.api_filter_options, name='api_filter_options'),
    path('commercial/insights/<str:state_code>/api/sample-data/', views.api_sample_data, name='api_sample_data'),
    path('commercial/insights/<str:state_code>/api/state-overview/', views.api_state_overview, name='api_state_overview'),
    path('commercial/insights/<str:state_code>/', views.commercial_rate_insights_state, name='commercial_rate_insights_state'),
    path('commercial/insights/<str:state_code>/compare/', views.commercial_rate_insights_compare, name='commercial_rate_insights_compare'),
    path('commercial/insights/<str:state_code>/custom-network/', views.custom_network_analysis, name='custom_network_analysis'),
    path('admin/activity/', views.user_activity_dashboard, name='user_activity_dashboard'),
]
