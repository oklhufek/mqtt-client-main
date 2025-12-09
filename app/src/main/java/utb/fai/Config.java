package utb.fai;

public class Config {

    // zakladni konfigurace pripojeni
    public static final String BROKER = "tcp://localhost:1883";
    public static final String CLIENT_ID = "mqtt-client";

    // topic pro prichozi zpravy
    public static final String TOPIC_IN = "topic/device1/in";

    // topic pro odchozi zpravy
    public static final String TOPIC_OUT = "topic/device1/out";

    // nazvy pozadavku (pro prichozi zpravy)
    public static final String REQUEST_GET_HUMIDITY = "get-humidity";
    public static final String REQUEST_GET_STATUS = "get-status";
    public static final String REQUEST_START_IRRIGATION = "start-irrigation";
    public static final String REQUEST_STOP_IRRIGATION = "stop-irrigation";

    // nazvy odpovedi (pro odchozi zpravy)
    public static final String RESPONSE_HUMIDITY = "humidity";
    public static final String RESPONSE_STATUS = "status";
    public static final String RESPONSE_FAULT = "fault";

}
