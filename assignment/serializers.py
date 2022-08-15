from rest_framework import serializers
from .models import WeatherData, Statistics, YieldData


class YieldDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = YieldData
        fields = '__all__'


class WeatherDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = WeatherData
        fields = '__all__'


class StatisticsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Statistics
        fields = '__all__'

