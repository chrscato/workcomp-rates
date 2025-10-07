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
    path('commercial/insights/dataset-review/data/', views.dataset_review_data, name='dataset_review_data'),
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
    # TIN and Provider Lookup
    path('tin-provider-lookup/', views.tin_provider_lookup, name='tin_provider_lookup'),
    path('tin-provider-lookup/ajax/', views.tin_provider_lookup_ajax, name='tin_provider_lookup_ajax'),
    
        # Rate Lookup
        path('rate-lookup/', views.rate_lookup_home, name='rate_lookup_home'),
        path('rate-lookup/tin/', views.rate_lookup_tin_lookup, name='rate_lookup_tin_lookup'),
        path('rate-lookup/tin/<str:tin_value>/', views.rate_lookup_tin_details, name='rate_lookup_tin_details'),
        path('rate-lookup/episodes/', views.rate_lookup_episodes_care, name='rate_lookup_episodes_care'),
        path('rate-lookup/explorer/', views.rate_lookup_data_explorer, name='rate_lookup_data_explorer'),
        path('rate-lookup/analyze/', views.rate_analyzer, name='rate_analyzer'),
        path('rate-lookup/tile-analyzer/', views.tile_analyzer, name='tile_analyzer'),
        path('rate-lookup/tile-analyzer/download-csv/', views.tile_analyzer_download_csv, name='tile_analyzer_download_csv'),
]
