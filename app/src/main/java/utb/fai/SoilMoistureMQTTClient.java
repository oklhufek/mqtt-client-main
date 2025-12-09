package utb.fai;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import utb.fai.API.HumiditySensor;
import utb.fai.API.IrrigationSystem;
import utb.fai.Types.FaultType;

/**
 * Trida MQTT klienta pro mereni vhlkosti pudy a rizeni zavlazovaciho systemu.
 *
 * V teto tride implementuje MQTT klienta
 */
public class SoilMoistureMQTTClient {

    private static final long HUMIDITY_INTERVAL_MS = 10_000L;
    private static final long IRRIGATION_TIMEOUT_MS = 30_000L;

    private MqttClient client;
    private final HumiditySensor humiditySensor;
    private final IrrigationSystem irrigationSystem;

    private ScheduledExecutorService executor;
    private volatile long lastIrrigationStartCommandTime = 0L;

    /**
     * Vytvori instacni tridy MQTT klienta pro mereni vhlkosti pudy a rizeni
     * zavlazovaciho systemu
     *
     * @param sensor     Senzor vlhkosti
     * @param irrigation Zarizeni pro zavlahu pudy
     */
    public SoilMoistureMQTTClient(HumiditySensor sensor, IrrigationSystem irrigation) {
        this.humiditySensor = sensor;
        this.irrigationSystem = irrigation;
    }

    /**
     * Metoda pro spusteni klienta
     */
    public void start() {
        try {
            this.client = new MqttClient(Config.BROKER, Config.CLIENT_ID);

            this.client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    // v ramci ukolu jen vypis chyby a ukonceni planovace
                    cause.printStackTrace();
                    if (executor != null) {
                        executor.shutdownNow();
                    }
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    handleIncomingMessage(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // nevyuzito
                }
            });

            this.client.connect();
            this.client.subscribe(Config.TOPIC_IN);

            this.executor = Executors.newScheduledThreadPool(2);

            // periodicke odesilani vlhkosti (prvni hned po startu)
            this.executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    sendHumiditySafe();
                }
            }, 0L, HUMIDITY_INTERVAL_MS, TimeUnit.MILLISECONDS);

            // hlidani casoveho limitu zavlahy
            this.executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    checkIrrigationTimeout();
                }
            }, 1L, 1_000L, TimeUnit.MILLISECONDS);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void handleIncomingMessage(String topic, MqttMessage message) {
        if (!Config.TOPIC_IN.equals(topic)) {
            return;
        }

        String payload = new String(message.getPayload(), StandardCharsets.UTF_8).trim();

        if (Config.REQUEST_GET_HUMIDITY.equals(payload)) {
            sendHumiditySafe();
        } else if (Config.REQUEST_GET_STATUS.equals(payload)) {
            sendStatus();
        } else if (Config.REQUEST_START_IRRIGATION.equals(payload)) {
            handleStartIrrigation();
        } else if (Config.REQUEST_STOP_IRRIGATION.equals(payload)) {
            handleStopIrrigation();
        }
    }

    private void sendHumiditySafe() {
        try {
            float value = this.humiditySensor.readRAWValue();

            if (this.humiditySensor.hasFault()) {
                sendFault(FaultType.HUMIDITY_SENSOR_FAULT);
            }

            String msg = Config.RESPONSE_HUMIDITY + ";" + value;
            publish(msg);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void sendStatus() {
        try {
            if (this.irrigationSystem.hasFault()) {
                sendFault(FaultType.IRRIGATION_SYSTEM_FAULT);
            }

            if (this.irrigationSystem.isActive()) {
                publish(Config.RESPONSE_STATUS + ";irrigation_on");
            } else {
                publish(Config.RESPONSE_STATUS + ";irrigation_off");
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void handleStartIrrigation() {
        this.lastIrrigationStartCommandTime = System.currentTimeMillis();

        this.irrigationSystem.activate();

        try {
            if (this.irrigationSystem.hasFault()) {
                sendFault(FaultType.IRRIGATION_SYSTEM_FAULT);
                return;
            }

            if (this.irrigationSystem.isActive()) {
                publish(Config.RESPONSE_STATUS + ";irrigation_on");
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void handleStopIrrigation() {
        this.irrigationSystem.deactivate();

        try {
            if (this.irrigationSystem.hasFault()) {
                sendFault(FaultType.IRRIGATION_SYSTEM_FAULT);
                return;
            }

            if (!this.irrigationSystem.isActive()) {
                publish(Config.RESPONSE_STATUS + ";irrigation_off");
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void checkIrrigationTimeout() {
        if (!this.irrigationSystem.isActive()) {
            return;
        }

        long last = this.lastIrrigationStartCommandTime;
        if (last <= 0L) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now - last >= IRRIGATION_TIMEOUT_MS) {
            this.irrigationSystem.deactivate();
            try {
                if (this.irrigationSystem.hasFault()) {
                    sendFault(FaultType.IRRIGATION_SYSTEM_FAULT);
                    return;
                }

                if (!this.irrigationSystem.isActive()) {
                    publish(Config.RESPONSE_STATUS + ";irrigation_off");
                }
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendFault(FaultType type) throws MqttException {
        String msg = Config.RESPONSE_FAULT + ";" + type.toString();
        publish(msg);
    }

    private void publish(String payload) throws MqttException {
        if (this.client == null || !this.client.isConnected()) {
            return;
        }
        MqttMessage mqttMessage = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        mqttMessage.setQos(0);
        this.client.publish(Config.TOPIC_OUT, mqttMessage);
    }

}
