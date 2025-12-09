package utb.fai.Types;

/**
 * Typ chyby, ktera muze nastat na iot zarizeni
 * 
 * ZDE NEMENIT ZADNOU CAST KODU
 */
public enum FaultType {

    // selhani cidla
    HUMIDITY_SENSOR_FAULT("HUMIDITY_SENSOR"),

    // selhani zavlazovaciho systemu
    IRRIGATION_SYSTEM_FAULT("IRRIGATION_SYSTEM");

    
    private final String name;

    private FaultType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
