from django.db import models


# Create your models here.

class WeatherData(models.Model):
    weather_station = models.CharField(primary_key=True, max_length=100)
    created_date = models.DateField()
    max_temp = models.IntegerField(null=True)
    min_temp = models.IntegerField(null=True)
    precipitation = models.IntegerField(null=True)

    class Meta:
        # unique_together = (('weather_station', 'created_date'))
        constraints = [
            models.UniqueConstraint(fields=['weather_station', 'created_date'], name='unique_wxdata')]
        db_table = 'weatherdata'
        managed = False


class YieldData(models.Model):
    yield_year = models.IntegerField(primary_key=True)
    total_yield = models.IntegerField(null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['yield_year'], name='unique_yieldyear')]
        db_table = 'yielddata'
        managed = False


class Statistics(models.Model):
    weather_station = models.CharField(primary_key=True, max_length=100)
    yield_year = models.IntegerField()
    avg_max_temp = models.FloatField(null=True)
    avg_min_temp = models.FloatField(null=True)
    total_precipitation = models.FloatField(null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['weather_station', 'yield_year'], name='unique_stats')]
        db_table = 'statistics'
        managed = False
