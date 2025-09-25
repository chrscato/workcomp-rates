from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('dashboard/', views.commercial_rate_insights_tile, name='dashboard'),  # Main entry point
    path('commercial/insights/', views.commercial_rate_insights_tile, name='commercial_rate_insights'),
    path('transparency/<str:state>/', views.transparency_dashboard, name='transparency'),
    path('transparency/<str:state>/analysis/', views.rate_analysis, name='rate_analysis'),
    path('benchmarks/compare/', views.benchmark_comparison, name='benchmark_compare'),
    path('steerage/preview/', views.steerage_preview, name='steerage_preview'),  # Stage 3 preview
    path('commercial/insights/data-availability/', views.data_availability_overview, name='data_availability_overview'),
    path('commercial/insights/data-availability-test/', views.data_availability_test, name='data_availability_overview_test'),
    path('commercial/insights/dataset-review/loading/', views.dataset_review_loading, name='dataset_review_loading'),
    path('commercial/insights/dataset-review/map/', views.dataset_review_map, name='dataset_review_map'),
    path('commercial/insights/dataset-review/filtered/', views.dataset_review_filtered, name='dataset_review_filtered'),
    path('commercial/insights/dataset-review/', views.dataset_review, name='dataset_review'),
    path('debug/filter-options/', views.debug_filter_options, name='debug_filter_options'),
    path('debug/s3-connection/', views.debug_s3_connection, name='debug_s3_connection'),
    # Legacy views (keeping for backward compatibility)
    path('commercial/insights/legacy/', views.commercial_rate_insights, name='commercial_rate_insights_legacy'),
    path('commercial/insights/map/', views.commercial_rate_insights_map, name='commercial_rate_insights_map'),
    path('commercial/insights/<str:state_code>/npi-selection/', views.npi_type_selection, name='npi_type_selection'),
    path('commercial/insights/<str:state_code>/overview/', views.commercial_rate_insights_overview, name='commercial_rate_insights_overview'),
    path('commercial/insights/<str:state_code>/overview-simple/', views.commercial_rate_insights_overview_simple, name='commercial_rate_insights_overview_simple'),
    path('commercial/insights/<str:state_code>/api/filter-options/', views.api_filter_options, name='api_filter_options'),
    path('commercial/insights/<str:state_code>/api/sample-data/', views.api_sample_data, name='api_sample_data'),
    path('commercial/insights/<str:state_code>/', views.commercial_rate_insights_state, name='commercial_rate_insights_state'),
    path('commercial/insights/<str:state_code>/compare/', views.commercial_rate_insights_compare, name='commercial_rate_insights_compare'),
    path('commercial/insights/<str:state_code>/custom-network/', views.custom_network_analysis, name='custom_network_analysis'),
    path('admin/activity/', views.user_activity_dashboard, name='user_activity_dashboard'),
]
