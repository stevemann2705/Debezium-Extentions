package in.stevemann.debeziumextentions;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;


/**
 * Relational Databases give DateTime and Date values as Epoch Date and Epoch Time respectively.
 * This custom converter allows user to handle all DateTime and Date fields as ISO Strings based on provided
 * set of date formats for each data type.
 *
 * @author Steve Mann
 */
@Slf4j
public class CustomDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

  private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE.withZone( ZoneId.of("UTC"));
  private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME.withZone( ZoneId.of("UTC"));
  private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME.withZone( ZoneId.of("UTC"));
  private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME.withZone( ZoneId.of("UTC"));

  private ZoneId timestampZoneId = ZoneId.systemDefault();

  @Override
  public void configure(Properties props) {
    readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
  }

  private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
    String settingValue = (String) properties.get(settingKey);
    if (settingValue == null || settingValue.length() == 0) {
      return;
    }
    try {
      callback.accept(settingValue.trim());
    } catch (IllegalArgumentException | DateTimeException e) {
      log.error("The \"{}\" setting is illegal:{}", settingKey, settingValue);
      throw e;
    }
  }

  @Override
  public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
    String columnType = column.typeName().toUpperCase();
    log.info("GOT COLUMN TYPE: " + columnType);
    SchemaBuilder schemaBuilder = null;
    Converter converter = null;
    if ("DATE".equals(columnType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertDate;
    }
    if ("TIME".equals(columnType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertTime;
    }
    if ("DATETIME".equals(columnType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertDateTime;
    }
    if ("TIMESTAMP".equals(columnType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertTimestamp;
    }
    if ("TIMESTAMPTZ".equals(columnType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertTimestamp;
    }
    if (schemaBuilder != null) {
      registration.register(schemaBuilder, converter);
      log.info("register converter for columnType {} to schema {}", columnType, schemaBuilder.name());
    }
  }

  private String convertDate(Object input) {
    log.info("Inside convert date");
    log.info("input type: " + input.getClass());
    if (input instanceof LocalDate) {
      return dateFormatter.format((LocalDate) input);
    }
    if (input instanceof Integer) {
      LocalDate date = LocalDate.ofEpochDay((Integer) input);
      return dateFormatter.format(date);
    }
    if (input instanceof java.sql.Timestamp) {
      return dateFormatter.format(((Timestamp) input).toLocalDateTime());
    }
    if (input instanceof java.sql.Date) {
      return dateFormatter.format(((java.sql.Date) input).toLocalDate());
    }
    return null;
  }

  private String convertTime(Object input) {
    log.info("Inside convert time");
    log.info("input type: " + input.getClass());
    if (input instanceof Duration) {
      Duration duration = (Duration) input;
      long seconds = duration.getSeconds();
      int nano = duration.getNano();
      LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
      return timeFormatter.format(time);
    }
    if (input instanceof java.sql.Timestamp) {
      return timeFormatter.format(((Timestamp) input).toLocalDateTime());
    }
    if (input instanceof java.sql.Time) {
      return timeFormatter.format(((java.sql.Time) input).toLocalTime());
    }
    return null;
  }

  private String convertDateTime(Object input) {
    log.info("Inside convert date time");
    log.info("input type: " + input.getClass());
    if (input instanceof LocalDateTime) {
      return datetimeFormatter.format((LocalDateTime) input);
    }
    if (input instanceof java.sql.Timestamp) {
      return datetimeFormatter.format(((Timestamp) input).toLocalDateTime());
    }
    return null;
  }

  private String convertTimestamp(Object input) {
    log.info("Inside convert timestamp");
    log.info("input type: " + input.getClass());
    if (input instanceof ZonedDateTime) {
      ZonedDateTime zonedDateTime = (ZonedDateTime) input;
      LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
      return timestampFormatter.format(localDateTime);
    }
    if (input instanceof java.sql.Timestamp) {
      return timestampFormatter.format(((Timestamp) input).toLocalDateTime());
    }
    return null;
  }

}
