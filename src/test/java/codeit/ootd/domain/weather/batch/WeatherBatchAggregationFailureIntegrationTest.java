package codeit.ootd.domain.weather.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import codeit.ootd.domain.weather.entity.Weather;
import codeit.ootd.domain.weather.entity.WeatherForecast;
import codeit.ootd.domain.weather.entity.WeatherLocation;
import codeit.ootd.domain.weather.repository.WeatherForecastRepository;
import codeit.ootd.domain.weather.repository.WeatherLocationRepository;
import codeit.ootd.domain.weather.repository.WeatherRepository;
import codeit.ootd.domain.weather.service.WeatherForecastCollector;
import codeit.ootd.domain.weather.service.WeatherService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate"
})
@ActiveProfiles("test")
class WeatherBatchAggregationFailureIntegrationTest {

    private static final ZoneId KOREA_ZONE =
        ZoneId.of("Asia/Seoul");

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("ootd_test")
            .withUsername("test")
            .withPassword("test");

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    @Qualifier("weatherCollectionJob")
    private Job weatherCollectionJob;

    @Autowired
    private WeatherLocationRepository weatherLocationRepository;

    @Autowired
    private WeatherForecastRepository weatherForecastRepository;

    @Autowired
    private WeatherRepository weatherRepository;

    @Autowired
    private Clock clock;

    @MockitoBean
    private WeatherForecastCollector weatherForecastCollector;

    @MockitoBean
    private WeatherBatchRateLimiter weatherBatchRateLimiter;

    @MockitoSpyBean
    private WeatherService weatherService;

    @Test
    @DisplayName("3일 차 집계 실패 시 이전 집계는 유지되고 10분 후 재시도를 예약한다")
    void aggregationFailurePreservesPreviousDaysAndSchedulesRetry()
        throws Exception {

        Instant now = clock.instant();

        LocalDate today = LocalDate.ofInstant(
            now,
            KOREA_ZONE
        );

        WeatherLocation location = new WeatherLocation(
            "test-" + UUID.randomUUID()
                .toString()
                .substring(0, 20),
            37.5665,
            126.9780,
            now.minus(Duration.ofHours(1))
        );

        location.completeCollection(
            now.minus(Duration.ofHours(3)),
            now.minus(Duration.ofHours(1))
        );

        location = weatherLocationRepository.saveAndFlush(location);

        UUID locationId = location.getId();

        weatherForecastRepository.saveAllAndFlush(List.of(
            forecast(locationId, today, now),
            forecast(locationId, today.plusDays(1), now)
        ));

        doThrow(new IllegalStateException("3일 차 집계 실패"))
            .when(weatherService)
            .calculateAndSaveDailyWeather(
                any(WeatherLocation.class),
                eq(today.plusDays(2))
            );

        JobExecution execution = jobOperator.start(
            weatherCollectionJob,
            new JobParametersBuilder()
                .addString("requestId", UUID.randomUUID().toString())
                .toJobParameters()
        );

        assertThat(execution.getStatus())
            .isEqualTo(BatchStatus.FAILED);

        List<Weather> savedWeathers = weatherRepository
            .findByLocationIdAndWeatherDateBetweenOrderByWeatherDateAsc(
                locationId,
                today,
                today.plusDays(4)
            );

        assertThat(savedWeathers)
            .extracting(Weather::getWeatherDate)
            .containsExactly(
                today,
                today.plusDays(1)
            );

        WeatherLocation updatedLocation = weatherLocationRepository
            .findById(locationId)
            .orElseThrow();

        assertThat(updatedLocation.getLastCollectedAt())
            .isEqualTo(now.minus(Duration.ofHours(3)));

        assertThat(updatedLocation.getNextRefreshAt())
            .isAfter(now.plus(Duration.ofMinutes(9)))
            .isBefore(now.plus(Duration.ofMinutes(11)));

        verify(weatherForecastCollector)
            .collectForecasts(any(WeatherLocation.class));

        verify(weatherService, never())
            .calculateAndSaveDailyWeather(
                any(WeatherLocation.class),
                eq(today.plusDays(3))
            );

        verify(weatherService, never())
            .calculateAndSaveDailyWeather(
                any(WeatherLocation.class),
                eq(today.plusDays(4))
            );
    }

    private WeatherForecast forecast(
        UUID locationId,
        LocalDate date,
        Instant collectedAt
    ) {
        return new WeatherForecast(
            locationId,
            date.atTime(12, 0)
                .atZone(KOREA_ZONE)
                .toInstant(),
            collectedAt,
            18.0,
            (short) 60,
            800,
            0.2,
            0.0,
            0.0,
            2.0
        );
    }
}
