from .models import WeatherData, YieldData, Statistics
from .serializers import YieldDataSerializer, WeatherDataSerializer, StatisticsSerializer
from rest_framework import generics
from django_filters.rest_framework import DjangoFilterBackend


class StatisticsListView(generics.ListAPIView):
    queryset = Statistics.objects.all()
    serializer_class = StatisticsSerializer


class WeatherDataListView(generics.ListAPIView):
    queryset = WeatherData.objects.all()
    serializer_class = WeatherDataSerializer
    #search_fields = ('=weather_station')
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['weather_station', 'created_date']


class YieldDataListView(generics.ListAPIView):
    queryset = YieldData.objects.all()
    serializer_class = YieldDataSerializer
